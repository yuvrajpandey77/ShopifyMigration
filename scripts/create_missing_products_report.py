import pandas as pd
import sys
import re
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.migration import normalize_product_name, determine_product_group_id

def get_all_migrated_products():
    """Get all products migrated in batches 1-10."""
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
    
    migrated_handles = set()
    migrated_base_names = set()
    migrated_products_info = {}  # Store product info
    
    for batch_file in batch_files:
        try:
            df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
            
            # Get handles
            handles = {h for h in df['Handle'].astype(str).str.strip().unique() if h and h.lower() != 'nan'}
            migrated_handles.update(handles)
            
            # Get parent rows for titles
            parent_rows = df[df['Title'].astype(str).str.strip() != '']
            if not parent_rows.empty:
                for _, row in parent_rows.iterrows():
                    handle = str(row['Handle']).strip()
                    title = str(row['Title']).strip()
                    base_name = normalize_product_name(title)
                    migrated_base_names.add(base_name)
                    
                    # Store product info
                    if handle not in migrated_products_info:
                        migrated_products_info[handle] = {
                            'title': title,
                            'base_name': base_name,
                            'has_description': False,
                            'has_price': False
                        }
                    
                    # Check description
                    desc_cols = [col for col in df.columns if 'description' in col.lower() or 'body' in col.lower()]
                    if desc_cols:
                        desc = str(row[desc_cols[0]]).strip()
                        if desc and desc.lower() != 'nan' and desc != '':
                            migrated_products_info[handle]['has_description'] = True
                    
                    # Check price
                    price_fields = ['Price', 'Variant Price']
                    for field in price_fields:
                        if field in row:
                            price_str = str(row[field]).strip()
                            if price_str and price_str.lower() != 'nan':
                                try:
                                    price = float(price_str.replace('$', '').replace(',', '').strip())
                                    if price > 0:
                                        migrated_products_info[handle]['has_price'] = True
                                        break
                                except:
                                    pass
            else:
                # No parent rows - use handles
                for handle in handles:
                    product_name = handle.replace('-', ' ').title()
                    base_name = normalize_product_name(product_name)
                    migrated_base_names.add(base_name)
                    if handle not in migrated_products_info:
                        migrated_products_info[handle] = {
                            'title': product_name,
                            'base_name': base_name,
                            'has_description': False,
                            'has_price': False
                        }
        except Exception as e:
            print(f"Error reading {batch_file}: {e}")
    
    return migrated_handles, migrated_base_names, migrated_products_info

def analyze_source_products(source_file: str, migrated_base_names: set):
    """Analyze source products to find missing ones and their field status."""
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
    
    # Group by product
    missing_products = []
    missing_with_all_fields = []
    missing_missing_fields = []
    
    for group_id, group_df in df.groupby('__ProductGroupID', sort=False):
        base_name = group_df['__BaseName'].iloc[0] if not group_df['__BaseName'].empty else ''
        
        # Skip if already migrated
        if base_name in migrated_base_names:
            continue
        
        # Get original product name
        original_name = group_df['Name'].iloc[0] if 'Name' in group_df.columns else base_name
        
        # Check fields
        has_price = group_df.apply(row_has_price, axis=1).any()
        has_image = group_df.apply(row_has_image, axis=1).any()
        has_description = group_df.apply(row_has_description, axis=1).any()
        
        missing_info = {
            'product_name': str(original_name).strip(),
            'base_name': base_name,
            'has_price': has_price,
            'has_image': has_image,
            'has_description': has_description,
            'missing_fields': []
        }
        
        if not has_price:
            missing_info['missing_fields'].append('Price')
        if not has_image:
            missing_info['missing_fields'].append('Image')
        if not has_description:
            missing_info['missing_fields'].append('Description')
        
        missing_products.append(missing_info)
        
        if has_price and has_image and has_description:
            missing_with_all_fields.append(missing_info)
        else:
            missing_missing_fields.append(missing_info)
    
    return missing_products, missing_with_all_fields, missing_missing_fields

def create_report(migrated_info: dict, missing_products: list, missing_with_all_fields: list, missing_missing_fields: list):
    """Create comprehensive report document."""
    report_file = "data/output/missing_products_comprehensive_report.txt"
    Path(report_file).parent.mkdir(parents=True, exist_ok=True)
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("COMPREHENSIVE PRODUCT MIGRATION REPORT\n")
        f.write("="*80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("\n")
        
        # Summary
        f.write("="*80 + "\n")
        f.write("EXECUTIVE SUMMARY\n")
        f.write("="*80 + "\n")
        f.write(f"Total products migrated (batches 1-10): {len(migrated_info):,}\n")
        f.write(f"Total products missing from migration: {len(missing_products):,}\n")
        f.write(f"Missing products WITH all fields (price, image, description): {len(missing_with_all_fields):,}\n")
        f.write(f"Missing products MISSING some fields: {len(missing_missing_fields):,}\n")
        f.write("\n")
        
        # Products without description in migrated
        f.write("="*80 + "\n")
        f.write("MIGRATED PRODUCTS WITHOUT DESCRIPTION (Batches 1-10)\n")
        f.write("="*80 + "\n")
        no_desc_count = 0
        for handle, info in sorted(migrated_info.items()):
            if not info['has_description']:
                no_desc_count += 1
                f.write(f"{no_desc_count}. {info['title']}\n")
        f.write(f"\nTotal: {no_desc_count} products without description\n")
        f.write("\n")
        
        # Products without price in migrated
        f.write("="*80 + "\n")
        f.write("MIGRATED PRODUCTS WITHOUT PRICE (Batches 1-10)\n")
        f.write("="*80 + "\n")
        no_price_count = 0
        for handle, info in sorted(migrated_info.items()):
            if not info['has_price']:
                no_price_count += 1
                f.write(f"{no_price_count}. {info['title']}\n")
        f.write(f"\nTotal: {no_price_count} products without price\n")
        f.write("\n")
        
        # Missing products with all fields
        f.write("="*80 + "\n")
        f.write("MISSING PRODUCTS WITH ALL FIELDS (Should have been migrated)\n")
        f.write("="*80 + "\n")
        f.write(f"These {len(missing_with_all_fields)} products have price, image, AND description but were NOT migrated:\n")
        f.write("\n")
        for i, product in enumerate(sorted(missing_with_all_fields, key=lambda x: x['product_name']), 1):
            f.write(f"{i}. {product['product_name']}\n")
        f.write("\n")
        
        # Missing products missing fields
        f.write("="*80 + "\n")
        f.write("MISSING PRODUCTS MISSING REQUIRED FIELDS\n")
        f.write("="*80 + "\n")
        f.write(f"These {len(missing_missing_fields)} products are missing from migration because they lack required fields:\n")
        f.write("\n")
        
        # Group by missing field
        missing_price_only = [p for p in missing_missing_fields if 'Price' in p['missing_fields'] and 'Image' not in p['missing_fields'] and 'Description' not in p['missing_fields']]
        missing_image_only = [p for p in missing_missing_fields if 'Image' in p['missing_fields'] and 'Price' not in p['missing_fields'] and 'Description' not in p['missing_fields']]
        missing_desc_only = [p for p in missing_missing_fields if 'Description' in p['missing_fields'] and 'Price' not in p['missing_fields'] and 'Image' not in p['missing_fields']]
        missing_price_image = [p for p in missing_missing_fields if 'Price' in p['missing_fields'] and 'Image' in p['missing_fields']]
        missing_all = [p for p in missing_missing_fields if len(p['missing_fields']) == 3]
        
        f.write(f"\nMissing ONLY Price ({len(missing_price_only)} products):\n")
        for i, product in enumerate(sorted(missing_price_only, key=lambda x: x['product_name']), 1):
            f.write(f"  {i}. {product['product_name']}\n")
        
        f.write(f"\nMissing ONLY Image ({len(missing_image_only)} products):\n")
        for i, product in enumerate(sorted(missing_image_only, key=lambda x: x['product_name']), 1):
            f.write(f"  {i}. {product['product_name']}\n")
        
        f.write(f"\nMissing ONLY Description ({len(missing_desc_only)} products):\n")
        for i, product in enumerate(sorted(missing_desc_only, key=lambda x: x['product_name']), 1):
            f.write(f"  {i}. {product['product_name']}\n")
        
        f.write(f"\nMissing Price AND Image ({len(missing_price_image)} products):\n")
        for i, product in enumerate(sorted(missing_price_image, key=lambda x: x['product_name']), 1):
            f.write(f"  {i}. {product['product_name']}\n")
        
        f.write(f"\nMissing ALL Fields ({len(missing_all)} products):\n")
        for i, product in enumerate(sorted(missing_all, key=lambda x: x['product_name']), 1):
            f.write(f"  {i}. {product['product_name']}\n")
        
        f.write("\n")
        
        # All missing products (complete list)
        f.write("="*80 + "\n")
        f.write("COMPLETE LIST OF ALL MISSING PRODUCTS\n")
        f.write("="*80 + "\n")
        for i, product in enumerate(sorted(missing_products, key=lambda x: x['product_name']), 1):
            missing_str = ', '.join(product['missing_fields']) if product['missing_fields'] else 'None (has all fields)'
            f.write(f"{i}. {product['product_name']} [Missing: {missing_str}]\n")
    
    return report_file

def main():
    source_file = "/home/yuvraj/Documents/products.csv"
    
    print("="*80)
    print("CREATING COMPREHENSIVE MISSING PRODUCTS REPORT")
    print("="*80)
    
    # Get migrated products
    print("\n1. Analyzing migrated products (batches 1-10)...")
    migrated_handles, migrated_base_names, migrated_info = get_all_migrated_products()
    print(f"   Found {len(migrated_info):,} migrated products")
    
    # Analyze source
    print("\n2. Analyzing source products...")
    missing_products, missing_with_all_fields, missing_missing_fields = analyze_source_products(source_file, migrated_base_names)
    print(f"   Found {len(missing_products):,} missing products")
    print(f"   Missing with all fields: {len(missing_with_all_fields):,}")
    print(f"   Missing with missing fields: {len(missing_missing_fields):,}")
    
    # Create report
    print("\n3. Creating comprehensive report...")
    report_file = create_report(migrated_info, missing_products, missing_with_all_fields, missing_missing_fields)
    print(f"   ✅ Report saved to: {report_file}")
    
    # Print summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Migrated products: {len(migrated_info):,}")
    print(f"Missing products: {len(missing_products):,}")
    print(f"  - With all fields (should migrate): {len(missing_with_all_fields):,}")
    print(f"  - Missing some fields: {len(missing_missing_fields):,}")
    
    # Count migrated products without description/price
    no_desc = sum(1 for info in migrated_info.values() if not info['has_description'])
    no_price = sum(1 for info in migrated_info.values() if not info['has_price'])
    print(f"\nMigrated products issues:")
    print(f"  - Without description: {no_desc}")
    print(f"  - Without price: {no_price}")

if __name__ == "__main__":
    main()

