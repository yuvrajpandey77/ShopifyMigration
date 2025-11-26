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
    # Batches 1-5 are named "batch_X_of_5.csv"
    for i in range(1, 6):
        batch_file = f'data/output/shopify_products_batch_{i}_of_5.csv'
        if Path(batch_file).exists():
            batch_files.append(batch_file)
    # Batches 6-10 are named "batch_X_of_10.csv"
    for i in range(6, 11):
        batch_file = f'data/output/shopify_products_batch_{i}_of_10.csv'
        if Path(batch_file).exists():
            batch_files.append(batch_file)
    
    migrated_handles = set()
    migrated_base_names = set()
    
    print("="*80)
    print("IDENTIFYING ALL MIGRATED PRODUCTS (BATCHES 1-10)")
    print("="*80)
    
    for batch_file in batch_files:
        try:
            df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
            # Get unique handles (each handle = one product)
            handles = {h for h in df['Handle'].astype(str).str.strip().unique() if h and h.lower() != 'nan'}
            migrated_handles.update(handles)
            
            # Also get base names from titles if available
            parent_rows = df[df['Title'].astype(str).str.strip() != '']
            if not parent_rows.empty:
                titles = {t for t in parent_rows['Title'].astype(str).str.strip().unique() if t and t.lower() != 'nan'}
                base_names = {normalize_product_name(t) for t in titles if normalize_product_name(t)}
                migrated_base_names.update(base_names)
        except Exception as e:
            logger.warning(f"Error reading {batch_file}: {e}")
    
    print(f"Found {len(migrated_handles):,} already migrated products (by Handle)")
    print(f"Found {len(migrated_base_names):,} already migrated base products")
    
    return migrated_handles, migrated_base_names

def find_more_products(source_file: str, migrated_handles: set, migrated_base_names: set, require_description: bool = False):
    """Find products that can still be migrated - with relaxed requirements."""
    print("\n" + "="*80)
    print(f"FINDING MORE PRODUCTS (require_description={require_description})")
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
    
    # Check for description (optional)
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
    skipped_reasons = {'already_migrated': 0, 'missing_price': 0, 'missing_image': 0, 'missing_description': 0}
    
    for group_id, group_df in df.groupby('__ProductGroupID', sort=False):
        # Check if this group is already migrated
        base_name = group_df['__BaseName'].iloc[0] if not group_df['__BaseName'].empty else ''
        
        if base_name in migrated_base_names:
            skipped_reasons['already_migrated'] += 1
            continue
        
        # Check requirements
        has_price = group_df.apply(row_has_price, axis=1).any()
        has_image = group_df.apply(row_has_image, axis=1).any()
        has_description = group_df.apply(row_has_description, axis=1).any()
        
        # Price and image are REQUIRED
        if not has_price:
            skipped_reasons['missing_price'] += 1
            continue
        
        if not has_image:
            skipped_reasons['missing_image'] += 1
            continue
        
        # Description is optional if require_description=False
        if require_description and not has_description:
            skipped_reasons['missing_description'] += 1
            continue
        
        # This group is valid!
        remaining_groups.append((group_id, group_df.copy()))
    
    print(f"\nSkipped reasons:")
    print(f"  Already migrated: {skipped_reasons['already_migrated']:,}")
    print(f"  Missing price: {skipped_reasons['missing_price']:,}")
    print(f"  Missing image: {skipped_reasons['missing_image']:,}")
    if require_description:
        print(f"  Missing description: {skipped_reasons['missing_description']:,}")
    
    print(f"\nRemaining valid groups: {len(remaining_groups):,}")
    
    if not remaining_groups:
        print("❌ No more products found!")
        return pd.DataFrame()
    
    # Combine remaining groups
    remaining_df = pd.concat([group_df for _, group_df in remaining_groups]).sort_index()
    
    # Remove helper columns
    for col in ['__BaseName', '__ProductGroupID']:
        if col in remaining_df.columns:
            remaining_df = remaining_df.drop(columns=[col])
    
    print(f"Total rows in remaining products: {len(remaining_df):,}")
    
    return remaining_df

def main():
    source_file = "/home/yuvraj/Documents/products.csv"
    
    # Get already migrated products
    migrated_handles, migrated_base_names = get_all_migrated_products()
    
    # First, try with description required (current requirement)
    print("\n" + "="*80)
    print("ATTEMPT 1: Requiring Price + Image + Description")
    print("="*80)
    remaining_with_desc = find_more_products(source_file, migrated_handles, migrated_base_names, require_description=True)
    
    # Then, try without description requirement (more lenient)
    print("\n" + "="*80)
    print("ATTEMPT 2: Requiring Only Price + Image (Description Optional)")
    print("="*80)
    remaining_without_desc = find_more_products(source_file, migrated_handles, migrated_base_names, require_description=False)
    
    # Compare
    print("\n" + "="*80)
    print("COMPARISON")
    print("="*80)
    print(f"With description required: {len(remaining_with_desc):,} rows")
    print(f"Without description required: {len(remaining_without_desc):,} rows")
    
    if len(remaining_without_desc) > len(remaining_with_desc):
        extra = len(remaining_without_desc) - len(remaining_with_desc)
        print(f"\n✅ Found {extra:,} additional rows by making description optional!")
        print("   Recommendation: Create batches 11+ with description optional")
        return remaining_without_desc
    elif len(remaining_with_desc) > 0:
        print(f"\n✅ Found {len(remaining_with_desc):,} rows with all requirements!")
        print("   Recommendation: Create batches 11+ with all requirements")
        return remaining_with_desc
    else:
        print("\n❌ No additional products found with any combination of requirements")
        return pd.DataFrame()

if __name__ == "__main__":
    result = main()
    if not result.empty:
        print(f"\n✅ Ready to create additional batches with {len(result):,} rows")

