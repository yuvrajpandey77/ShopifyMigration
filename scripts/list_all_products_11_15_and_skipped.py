import pandas as pd
import sys
import re
from pathlib import Path

# Import migration functions
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.migration import normalize_product_name, determine_product_group_id

def get_products_from_batches_11_15():
    """Get all unique product names from batches 11-15."""
    print("="*80)
    print("EXTRACTING PRODUCTS FROM BATCHES 11-15")
    print("="*80)
    
    batch_files = []
    for i in range(11, 16):
        batch_file = f'data/output/shopify_products_batch_{i}_of_15.csv'
        if Path(batch_file).exists():
            batch_files.append(batch_file)
    
    all_products = set()
    
    for batch_file in batch_files:
        df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
        
        # Get unique handles (each handle = one product)
        handles = {h for h in df['Handle'].astype(str).str.strip().unique() if h and h.lower() != 'nan'}
        
        # Convert handles to product names (remove dashes, title case)
        for handle in handles:
            # Get the first row for this handle to get the actual product name
            handle_rows = df[df['Handle'] == handle]
            if not handle_rows.empty:
                # Try to get Title first
                title = handle_rows['Title'].iloc[0] if 'Title' in handle_rows.columns else ''
                if title and str(title).strip() and str(title).strip().lower() != 'nan':
                    base_name = normalize_product_name(str(title))
                    if base_name:
                        all_products.add(base_name)
                else:
                    # Fallback: convert handle to readable name
                    product_name = handle.replace('-', ' ').title()
                    base_name = normalize_product_name(product_name)
                    if base_name:
                        all_products.add(base_name)
    
    print(f"Found {len(all_products)} unique products in batches 11-15")
    return all_products

def get_skipped_products():
    """Get products that were skipped due to missing price/image/description."""
    print("\n" + "="*80)
    print("FINDING SKIPPED PRODUCTS (Missing Price/Image/Description)")
    print("="*80)
    
    source_file = "/home/yuvraj/Documents/products.csv"
    if not Path(source_file).exists():
        print(f"❌ Source file not found: {source_file}")
        return set()
    
    # Get all migrated products
    migrated_base_names = set()
    
    # Batches 1-5
    for i in range(1, 6):
        batch_file = f'data/output/shopify_products_batch_{i}_of_5.csv'
        if Path(batch_file).exists():
            try:
                df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
                parent_rows = df[df['Title'].astype(str).str.strip() != '']
                if not parent_rows.empty:
                    titles = {t for t in parent_rows['Title'].astype(str).str.strip().unique() if t and t.lower() != 'nan'}
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
                handles = {h for h in df['Handle'].astype(str).str.strip().unique() if h and h.lower() != 'nan'}
                for handle in handles:
                    product_name = handle.replace('-', ' ').title()
                    base_name = normalize_product_name(product_name)
                    if base_name:
                        migrated_base_names.add(base_name)
            except:
                pass
    
    # Batches 11-15
    for i in range(11, 16):
        batch_file = f'data/output/shopify_products_batch_{i}_of_15.csv'
        if Path(batch_file).exists():
            try:
                df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
                handles = {h for h in df['Handle'].astype(str).str.strip().unique() if h and h.lower() != 'nan'}
                for handle in handles:
                    product_name = handle.replace('-', ' ').title()
                    base_name = normalize_product_name(product_name)
                    if base_name:
                        migrated_base_names.add(base_name)
            except:
                pass
    
    print(f"Total migrated products: {len(migrated_base_names):,}")
    
    # Read source file
    df = pd.read_csv(source_file, low_memory=False)
    df['__BaseName'] = df['Name'].apply(normalize_product_name)
    
    # Check requirements
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
    
    # Group by base name
    skipped_products = set()
    
    for base_name, group_df in df.groupby('__BaseName', sort=False):
        # Skip if already migrated
        if base_name in migrated_base_names:
            continue
        
        # Check if group has price and image
        has_price = group_df.apply(row_has_price, axis=1).any()
        has_image = group_df.apply(row_has_image, axis=1).any()
        
        # If missing price or image, it's skipped
        if not has_price or not has_image:
            # Get the original product name (not normalized)
            original_name = group_df['Name'].iloc[0] if 'Name' in group_df.columns else base_name
            skipped_products.add(str(original_name).strip())
    
    print(f"Found {len(skipped_products)} skipped products (missing price or image)")
    return skipped_products

def main():
    # Get products from batches 11-15
    products_11_15 = get_products_from_batches_11_15()
    
    # Get skipped products
    skipped_products = get_skipped_products()
    
    # Combine and sort
    all_products = sorted(products_11_15 | skipped_products)
    
    print("\n" + "="*80)
    print("FINAL LIST")
    print("="*80)
    print(f"\nTotal products: {len(all_products):,}")
    print(f"  - From batches 11-15: {len(products_11_15):,}")
    print(f"  - Skipped (missing price/image): {len(skipped_products):,}")
    
    # Save to file
    output_file = "data/output/products_list_11_15_and_skipped.txt"
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for i, product in enumerate(all_products, 1):
            f.write(f"{i}. {product}\n")
    
    print(f"\n✅ List saved to: {output_file}")
    print(f"\nFirst 50 products:")
    for i, product in enumerate(all_products[:50], 1):
        print(f"  {i}. {product}")
    
    if len(all_products) > 50:
        print(f"\n... and {len(all_products) - 50} more (see {output_file})")

if __name__ == "__main__":
    main()

