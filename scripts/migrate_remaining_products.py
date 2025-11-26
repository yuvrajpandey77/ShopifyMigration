import pandas as pd
import sys
import re
import math
from pathlib import Path
from loguru import logger
import yaml

# Import migration functions
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.migration import normalize_product_name, determine_product_group_id

def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        return {}

def get_already_migrated_products():
    """Get list of products already migrated in batches 1-5."""
    batch_files = [
        'data/output/shopify_products_batch_1_of_5.csv',
        'data/output/shopify_products_batch_2_of_5.csv',
        'data/output/shopify_products_batch_3_of_5.csv',
        'data/output/shopify_products_batch_4_of_5.csv',
        'data/output/shopify_products_batch_5_of_5.csv',
    ]
    
    migrated_handles = set()
    migrated_base_names = set()
    
    print("="*80)
    print("IDENTIFYING ALREADY MIGRATED PRODUCTS")
    print("="*80)
    
    for batch_file in batch_files:
        if not Path(batch_file).exists():
            continue
        
        df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
        parent_rows = df[df['Title'].astype(str).str.strip() != '']
        
        handles = {h for h in parent_rows['Handle'].astype(str).str.strip().unique() if h and h.lower() != 'nan'}
        titles = {t for t in parent_rows['Title'].astype(str).str.strip().unique() if t and t.lower() != 'nan'}
        base_names = {normalize_product_name(t) for t in titles if normalize_product_name(t)}
        
        migrated_handles.update(handles)
        migrated_base_names.update(base_names)
    
    print(f"Found {len(migrated_handles):,} already migrated products (by Handle)")
    print(f"Found {len(migrated_base_names):,} already migrated base products")
    
    return migrated_handles, migrated_base_names

def find_remaining_products(source_file: str, migrated_handles: set, migrated_base_names: set):
    """Find products in source that haven't been migrated yet."""
    print("\n" + "="*80)
    print("FINDING REMAINING PRODUCTS FROM SOURCE")
    print("="*80)
    
    if not Path(source_file).exists():
        print(f"❌ ERROR: Source file not found: {source_file}")
        return pd.DataFrame()
    
    df = pd.read_csv(source_file, low_memory=False)
    print(f"Total rows in source: {len(df):,}")
    
    # Compute base names and product group IDs
    df['__BaseName'] = df['Name'].apply(normalize_product_name)
    df['__ProductGroupID'] = df.apply(determine_product_group_id, axis=1)
    
    # Check for price
    def row_has_price(row: pd.Series) -> bool:
        for field in ['Regular price', 'Sale price', 'Price']:
            if field in row and pd.notna(row[field]):
                try:
                    price_str = str(row[field]).replace('$', '').replace(',', '').replace('₹', '').strip()
                    if price_str and price_str.lower() != 'nan':
                        price = float(price_str)
                        if price > 0:
                            return True
                except Exception:
                    continue
        return False
    
    # Check for image
    def row_has_image(row: pd.Series) -> bool:
        for field in ['Images', 'Image', 'images', 'image']:
            if field in row:
                img = row.get(field, '')
                if pd.notna(img):
                    img_str = str(img).strip()
                    if img_str and img_str.lower() != 'nan':
                        return True
        return False
    
    # Check for description
    def row_has_description(row: pd.Series) -> bool:
        desc_fields = ['Description', 'Short description', 'Body (HTML)', 'description', 'short_description']
        for field in desc_fields:
            if field in row:
                desc = row.get(field, '')
                if pd.notna(desc):
                    desc_str = str(desc).strip()
                    if desc_str and desc_str.lower() != 'nan' and desc_str.lower() != '':
                        return True
        return False
    
    # Group by product group
    remaining_groups = []
    already_migrated_groups = set()
    
    for group_id, group_df in df.groupby('__ProductGroupID', sort=False):
        # Check if this group is already migrated
        base_name = group_df['__BaseName'].iloc[0] if not group_df['__BaseName'].empty else ''
        
        # Check if base name is in migrated list
        if base_name in migrated_base_names:
            already_migrated_groups.add(group_id)
            continue
        
        # Check if group has price, image, and description
        has_price = group_df.apply(row_has_price, axis=1).any()
        has_image = group_df.apply(row_has_image, axis=1).any()
        has_description = group_df.apply(row_has_description, axis=1).any()
        
        # Only include if has ALL: price, image, AND description
        if has_price and has_image and has_description:
            remaining_groups.append((group_id, group_df.copy()))
        else:
            # Log why it's being skipped
            missing = []
            if not has_price:
                missing.append("price")
            if not has_image:
                missing.append("image")
            if not has_description:
                missing.append("description")
            logger.debug(f"Skipping group {group_id}: missing {', '.join(missing)}")
    
    print(f"Already migrated groups: {len(already_migrated_groups):,}")
    print(f"Remaining valid groups (with price, image, description): {len(remaining_groups):,}")
    
    if not remaining_groups:
        print("❌ No remaining products found that meet all requirements!")
        return pd.DataFrame()
    
    # Combine remaining groups into one DataFrame
    remaining_df = pd.concat([group_df for _, group_df in remaining_groups]).sort_index()
    
    # Remove helper columns
    for col in ['__BaseName', '__ProductGroupID']:
        if col in remaining_df.columns:
            remaining_df = remaining_df.drop(columns=[col])
    
    print(f"Total rows in remaining products: {len(remaining_df):,}")
    
    return remaining_df

def split_into_batches(df: pd.DataFrame, num_batches: int = 5):
    """Split remaining products into batches, keeping product groups together."""
    print("\n" + "="*80)
    print(f"SPLITTING INTO {num_batches} BATCHES")
    print("="*80)
    
    # Re-compute grouping (since we removed helper columns)
    df['__BaseName'] = df['Name'].apply(normalize_product_name)
    df['__ProductGroupID'] = df.apply(determine_product_group_id, axis=1)
    
    # Group by product group
    valid_groups = []
    for group_id, group_df in df.groupby('__ProductGroupID', sort=False):
        first_idx = group_df.index.min()
        valid_groups.append((first_idx, group_id, group_df.copy()))
    
    valid_groups.sort(key=lambda item: item[0])
    
    # Split into batches
    batch_dfs = []
    groups_per_batch = math.ceil(len(valid_groups) / num_batches) if valid_groups else 0
    
    for i in range(num_batches):
        start = i * groups_per_batch
        end = min((i + 1) * groups_per_batch, len(valid_groups))
        batch_groups = valid_groups[start:end]
        
        if not batch_groups:
            batch_df = pd.DataFrame(columns=df.columns)
        else:
            batch_df = pd.concat([group_df for _, _, group_df in batch_groups]).sort_index()
        
        # Remove helper columns
        for col in ['__BaseName', '__ProductGroupID']:
            if col in batch_df.columns:
                batch_df = batch_df.drop(columns=[col])
        
        batch_dfs.append(batch_df)
        print(f"Batch {i+1}: {len(batch_df):,} rows across {len(batch_groups)} product groups")
    
    return batch_dfs

def main():
    # Load config
    config = load_config()
    source_file = config.get('files', {}).get('source_csv', '/home/yuvraj/Documents/products.csv')
    output_dir = Path(config.get('files', {}).get('output_dir', './data/output'))
    shopify_template = config.get('files', {}).get('shopify_template', '/home/yuvraj/Documents/product_template_csv_unit_price.csv')
    mapping_config = 'config/field_mapping.json'
    
    # Get already migrated products
    migrated_handles, migrated_base_names = get_already_migrated_products()
    
    # Find remaining products
    remaining_df = find_remaining_products(source_file, migrated_handles, migrated_base_names)
    
    if remaining_df.empty:
        print("\n❌ No remaining products to migrate!")
        return
    
    # Determine number of batches needed
    # Estimate: if we have ~10,000 products and want ~300 per batch, we need ~33 batches
    # But let's start with 5 batches and see how many products we have
    estimated_products = len(remaining_df.groupby(remaining_df['Name'].apply(normalize_product_name)))
    num_batches = max(5, math.ceil(estimated_products / 300))  # ~300 products per batch
    
    print(f"\nEstimated {estimated_products:,} products remaining")
    print(f"Will create {num_batches} batches")
    
    # Split into batches
    batch_dfs = split_into_batches(remaining_df, num_batches)
    
    # Save batch source files
    temp_dir = Path('data/temp_batches')
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    batch_files = []
    for i, batch_df in enumerate(batch_dfs, 6):  # Start from batch 6
        if batch_df.empty:
            continue
        batch_file = temp_dir / f"batch_{i}_source.csv"
        batch_df.to_csv(batch_file, index=False)
        batch_files.append((i, str(batch_file)))
        print(f"Saved batch {i} source: {batch_file} ({len(batch_df)} rows)")
    
    print(f"\n✅ Created {len(batch_files)} new batch source files")
    print(f"Ready to migrate batches: {', '.join(str(i) for i, _ in batch_files)}")
    
    return batch_files

if __name__ == "__main__":
    main()

