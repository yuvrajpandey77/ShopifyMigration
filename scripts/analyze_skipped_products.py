import pandas as pd
import sys
from pathlib import Path

def analyze_skipped_products():
    """Analyze why products are being skipped during migration."""
    
    source_file = "/home/yuvraj/Documents/products.csv"
    
    print("="*80)
    print("ANALYZING WHY PRODUCTS ARE BEING SKIPPED")
    print("="*80)
    
    if not Path(source_file).exists():
        print(f"❌ ERROR: Source file not found: {source_file}")
        return
    
    # Read source file
    print(f"\nReading source file: {source_file}")
    df = pd.read_csv(source_file, low_memory=False)
    total_rows = len(df)
    print(f"Total rows: {total_rows:,}")
    
    # Check for price columns
    price_columns = ['Regular price', 'Sale price', 'Price', 'price']
    available_price_cols = [col for col in price_columns if col in df.columns]
    
    if not available_price_cols:
        print("⚠️  No price columns found. Available columns:")
        print(df.columns.tolist()[:20])
        return
    
    print(f"\nPrice columns found: {available_price_cols}")
    
    # Check for image columns
    image_columns = ['Images', 'Image', 'images', 'image']
    available_image_cols = [col for col in image_columns if col in df.columns]
    
    if not available_image_cols:
        print("⚠️  No image columns found. Available columns:")
        print(df.columns.tolist()[:20])
        return
    
    print(f"Image columns found: {available_image_cols}")
    
    # Function to check if row has valid price
    def row_has_price(row: pd.Series) -> bool:
        for field in available_price_cols:
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
    
    # Function to check if row has image
    def row_has_image(row: pd.Series) -> bool:
        for field in available_image_cols:
            img = row.get(field, '')
            if pd.notna(img):
                img_str = str(img).strip()
                if img_str and img_str.lower() != 'nan':
                    return True
        return False
    
    # Analyze each row
    print("\n" + "="*80)
    print("ANALYZING ROWS")
    print("="*80)
    
    has_price = df.apply(row_has_price, axis=1)
    has_image = df.apply(row_has_image, axis=1)
    has_both = has_price & has_image
    missing_price = ~has_price
    missing_image = ~has_image
    missing_both = ~has_both
    
    print(f"\nRows with valid price: {has_price.sum():,} ({has_price.sum()/total_rows*100:.2f}%)")
    print(f"Rows with image: {has_image.sum():,} ({has_image.sum()/total_rows*100:.2f}%)")
    print(f"Rows with BOTH price AND image: {has_both.sum():,} ({has_both.sum()/total_rows*100:.2f}%)")
    print(f"\nRows missing price: {missing_price.sum():,} ({missing_price.sum()/total_rows*100:.2f}%)")
    print(f"Rows missing image: {missing_image.sum():,} ({missing_image.sum()/total_rows*100:.2f}%)")
    print(f"Rows missing BOTH price AND image: {missing_both.sum():,} ({missing_both.sum()/total_rows*100:.2f}%)")
    
    # Group by product group (using Name as proxy)
    print("\n" + "="*80)
    print("ANALYZING BY PRODUCT GROUPS")
    print("="*80)
    
    name_col = 'Name' if 'Name' in df.columns else df.columns[0]
    
    # Group by product name (base analysis)
    grouped = df.groupby(name_col)
    
    groups_with_price = 0
    groups_with_image = 0
    groups_with_both = 0
    groups_missing_both = 0
    
    for name, group_df in grouped:
        group_has_price = group_df.apply(row_has_price, axis=1).any()
        group_has_image = group_df.apply(row_has_image, axis=1).any()
        group_has_both = group_has_price and group_has_image
        
        if group_has_price:
            groups_with_price += 1
        if group_has_image:
            groups_with_image += 1
        if group_has_both:
            groups_with_both += 1
        if not group_has_both:
            groups_missing_both += 1
    
    total_groups = len(grouped)
    
    print(f"\nTotal product groups (by Name): {total_groups:,}")
    print(f"Groups with at least one row having price: {groups_with_price:,} ({groups_with_price/total_groups*100:.2f}%)")
    print(f"Groups with at least one row having image: {groups_with_image:,} ({groups_with_image/total_groups*100:.2f}%)")
    print(f"Groups with BOTH price AND image: {groups_with_both:,} ({groups_with_both/total_groups*100:.2f}%)")
    print(f"Groups missing BOTH price AND image: {groups_missing_both:,} ({groups_missing_both/total_groups*100:.2f}%)")
    
    # Show sample of missing products
    print("\n" + "="*80)
    print("SAMPLE OF PRODUCTS BEING SKIPPED")
    print("="*80)
    
    # Products missing both
    missing_both_df = df[missing_both].copy()
    if not missing_both_df.empty:
        print(f"\nSample of rows missing BOTH price and image (first 10):")
        sample = missing_both_df.head(10)
        for idx, row in sample.iterrows():
            name = row.get(name_col, 'N/A')
            print(f"  - {name}")
    
    # Products missing price only
    missing_price_only = missing_price & has_image
    if missing_price_only.sum() > 0:
        print(f"\nRows missing price only (have image): {missing_price_only.sum():,}")
        sample = df[missing_price_only].head(5)
        for idx, row in sample.iterrows():
            name = row.get(name_col, 'N/A')
            print(f"  - {name}")
    
    # Products missing image only
    missing_image_only = missing_image & has_price
    if missing_image_only.sum() > 0:
        print(f"\nRows missing image only (have price): {missing_image_only.sum():,}")
        sample = df[missing_image_only].head(5)
        for idx, row in sample.iterrows():
            name = row.get(name_col, 'N/A')
            print(f"  - {name}")
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY - WHY PRODUCTS ARE NOT MIGRATED")
    print("="*80)
    print(f"\n✅ Products that WILL be migrated: {groups_with_both:,} groups ({groups_with_both/total_groups*100:.2f}%)")
    print(f"❌ Products that WILL NOT be migrated: {groups_missing_both:,} groups ({groups_missing_both/total_groups*100:.2f}%)")
    print(f"\nReason: Migration requires BOTH a valid price (> 0) AND at least one image URL")
    print(f"Products missing either requirement are filtered out during batch splitting.")

if __name__ == "__main__":
    analyze_skipped_products()

