import pandas as pd
import sys
import re
import math
from pathlib import Path
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.migration import normalize_product_name, determine_product_group_id

def get_all_migrated_products():
    """Get all products already migrated in batches 1-10."""
    migrated_base_names = set()
    
    # Batches 1-5
    for i in range(1, 6):
        batch_file = f'data/output/shopify_products_batch_{i}_of_5.csv'
        if Path(batch_file).exists():
            try:
                df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
                parent_rows = df[df['Title'].astype(str).str.strip() != '']
                if not parent_rows.empty:
                    titles = parent_rows['Title'].astype(str).str.strip().unique()
                    base_names = {normalize_product_name(t) for t in titles if normalize_product_name(t)}
                    migrated_base_names.update(base_names)
            except:
                pass
    
    # Batches 6-10
    for i in range(6, 11):
        batch_file = f'data/output/shopify_products_batch_{i}_of_10.csv'
        if Path(batch_file).exists():
            try:
                df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
                handles = df['Handle'].astype(str).str.strip().unique()
                for handle in handles:
                    if handle and handle.lower() != 'nan':
                        product_name = handle.replace('-', ' ').title()
                        base_name = normalize_product_name(product_name)
                        if base_name:
                            migrated_base_names.add(base_name)
            except:
                pass
    
    # Batches 11-15 (if they exist)
    for i in range(11, 16):
        batch_file = f'data/output/shopify_products_batch_{i}_of_15.csv'
        if Path(batch_file).exists():
            try:
                df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
                handles = df['Handle'].astype(str).str.strip().unique()
                for handle in handles:
                    if handle and handle.lower() != 'nan':
                        product_name = handle.replace('-', ' ').title()
                        base_name = normalize_product_name(product_name)
                        if base_name:
                            migrated_base_names.add(base_name)
            except:
                pass
    
    return migrated_base_names

def find_migratable_products(source_file: str, migrated_base_names: set):
    """Find products that can be migrated (have price, image, description)."""
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
                except:
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
    
    migratable_groups = []
    
    for group_id, group_df in df.groupby('__ProductGroupID', sort=False):
        base_name = group_df['__BaseName'].iloc[0] if not group_df['__BaseName'].empty else ''
        
        # Skip if already migrated
        if base_name in migrated_base_names:
            continue
        
        # Check if has ALL: price, image, AND description
        has_price = group_df.apply(row_has_price, axis=1).any()
        has_image = group_df.apply(row_has_image, axis=1).any()
        has_description = group_df.apply(row_has_description, axis=1).any()
        
        if has_price and has_image and has_description:
            migratable_groups.append((group_id, group_df.copy()))
    
    if not migratable_groups:
        return pd.DataFrame()
    
    remaining_df = pd.concat([group_df for _, group_df in migratable_groups]).sort_index()
    
    for col in ['__BaseName', '__ProductGroupID']:
        if col in remaining_df.columns:
            remaining_df = remaining_df.drop(columns=[col])
    
    return remaining_df

def split_into_batches(df: pd.DataFrame, num_batches: int = 6):
    """Split products into batches."""
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
    print("FINAL MIGRATION - 503 PRODUCTS WITH ALL FIELDS")
    print("="*80)
    
    # Get already migrated products
    print("\n1. Identifying already migrated products...")
    migrated_base_names = get_all_migrated_products()
    print(f"   Found {len(migrated_base_names):,} already migrated base products")
    
    # Find migratable products
    print("\n2. Finding products with all fields (price, image, description)...")
    migratable_df = find_migratable_products(source_file, migrated_base_names)
    
    if migratable_df.empty:
        print("   ❌ No migratable products found!")
        return
    
    print(f"   Found {len(migratable_df):,} rows in migratable products")
    
    # Estimate number of products
    estimated_products = len(migratable_df.groupby(migratable_df['Name'].apply(normalize_product_name)))
    print(f"   Estimated {estimated_products:,} unique products")
    
    # Determine number of batches (aim for ~80-100 products per batch)
    num_batches = max(6, math.ceil(estimated_products / 85))
    print(f"\n3. Splitting into {num_batches} batches...")
    
    # Split into batches
    batch_dfs = split_into_batches(migratable_df, num_batches)
    
    # Save batch source files
    temp_dir = Path('data/temp_batches')
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    batch_files = []
    start_batch = 11
    
    for i, batch_df in enumerate(batch_dfs):
        batch_num = start_batch + i
        if batch_df.empty:
            continue
        batch_file = temp_dir / f"batch_{batch_num}_source.csv"
        batch_df.to_csv(batch_file, index=False)
        batch_files.append((batch_num, str(batch_file)))
        print(f"   Saved batch {batch_num} source: {batch_file} ({len(batch_df)} rows)")
    
    print(f"\n✅ Created {len(batch_files)} new batch source files")
    print(f"   Ready to migrate batches: {', '.join(str(i) for i, _ in batch_files)}")
    
    return batch_files

if __name__ == "__main__":
    main()

