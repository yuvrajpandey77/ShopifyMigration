import pandas as pd
import sys
import re
import math
from pathlib import Path
from loguru import logger

# Import migration functions
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.migration import normalize_product_name, determine_product_group_id

def get_all_migrated_products():
    """Get all products already migrated in batches 1-10."""
    batch_files = []
    # Batches 1-5
    for i in range(1, 6):
        batch_file = f'data/output/shopify_products_batch_{i}_of_5.csv'
        if Path(batch_file).exists():
            batch_files.append(batch_file)
    # Batches 6-10
    for i in range(6, 11):
        batch_file = f'data/output/shopify_products_batch_{i}_of_10.csv'
        if Path(batch_file).exists():
            batch_files.append(batch_file)
    
    migrated_base_names = set()
    
    for batch_file in batch_files:
        try:
            df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
            # Get base names from titles if available
            parent_rows = df[df['Title'].astype(str).str.strip() != '']
            if not parent_rows.empty:
                titles = {t for t in parent_rows['Title'].astype(str).str.strip().unique() if t and t.lower() != 'nan'}
                base_names = {normalize_product_name(t) for t in titles if normalize_product_name(t)}
                migrated_base_names.update(base_names)
            else:
                # If no titles, use handles to create base names
                handles = {h for h in df['Handle'].astype(str).str.strip().unique() if h and h.lower() != 'nan'}
                for handle in handles:
                    base_name = handle.replace('-', ' ').title()
                    migrated_base_names.add(normalize_product_name(base_name))
        except Exception as e:
            logger.warning(f"Error reading {batch_file}: {e}")
    
    return migrated_base_names

def find_remaining_products(source_file: str, migrated_base_names: set):
    """Find remaining products with price + image (description optional)."""
    df = pd.read_csv(source_file, low_memory=False)
    df['__BaseName'] = df['Name'].apply(normalize_product_name)
    df['__ProductGroupID'] = df.apply(determine_product_group_id, axis=1)
    
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
    
    def row_has_image(row: pd.Series) -> bool:
        for field in ['Images', 'Image', 'images', 'image']:
            if field in row:
                img = row.get(field, '')
                if pd.notna(img):
                    img_str = str(img).strip()
                    if img_str and img_str.lower() != 'nan':
                        return True
        return False
    
    remaining_groups = []
    
    for group_id, group_df in df.groupby('__ProductGroupID', sort=False):
        base_name = group_df['__BaseName'].iloc[0] if not group_df['__BaseName'].empty else ''
        
        if base_name in migrated_base_names:
            continue
        
        has_price = group_df.apply(row_has_price, axis=1).any()
        has_image = group_df.apply(row_has_image, axis=1).any()
        
        if has_price and has_image:
            remaining_groups.append((group_id, group_df.copy()))
    
    if not remaining_groups:
        return pd.DataFrame()
    
    remaining_df = pd.concat([group_df for _, group_df in remaining_groups]).sort_index()
    
    for col in ['__BaseName', '__ProductGroupID']:
        if col in remaining_df.columns:
            remaining_df = remaining_df.drop(columns=[col])
    
    return remaining_df

def split_into_batches(df: pd.DataFrame, num_batches: int = 5):
    """Split remaining products into batches."""
    df['__BaseName'] = df['Name'].apply(normalize_product_name)
    df['__ProductGroupID'] = df.apply(determine_product_group_id, axis=1)
    
    valid_groups = []
    for group_id, group_df in df.groupby('__ProductGroupID', sort=False):
        first_idx = group_df.index.min()
        valid_groups.append((first_idx, group_id, group_df.copy()))
    
    valid_groups.sort(key=lambda item: item[0])
    
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
        
        for col in ['__BaseName', '__ProductGroupID']:
            if col in batch_df.columns:
                batch_df = batch_df.drop(columns=[col])
        
        batch_dfs.append(batch_df)
        print(f"Batch {len(batch_dfs) + 10}: {len(batch_df):,} rows across {len(batch_groups)} product groups")
    
    return batch_dfs

def main():
    source_file = "/home/yuvraj/Documents/products.csv"
    
    print("="*80)
    print("CREATING BATCHES 11+ FOR REMAINING PRODUCTS")
    print("="*80)
    
    # Get already migrated products
    migrated_base_names = get_all_migrated_products()
    print(f"\nAlready migrated base products: {len(migrated_base_names):,}")
    
    # Find remaining products
    remaining_df = find_remaining_products(source_file, migrated_base_names)
    
    if remaining_df.empty:
        print("\n❌ No remaining products found!")
        return
    
    print(f"\nFound {len(remaining_df):,} rows in remaining products")
    
    # Estimate number of batches needed (~300 products per batch)
    estimated_products = len(remaining_df.groupby(remaining_df['Name'].apply(normalize_product_name)))
    num_batches = max(5, math.ceil(estimated_products / 300))
    
    print(f"Estimated {estimated_products:,} products")
    print(f"Will create {num_batches} batches")
    
    # Split into batches
    batch_dfs = split_into_batches(remaining_df, num_batches)
    
    # Save batch source files
    temp_dir = Path('data/temp_batches')
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    batch_files = []
    for i, batch_df in enumerate(batch_dfs, 11):
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

