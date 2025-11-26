import pandas as pd
import sys
import re
from pathlib import Path

def normalize_product_name(product_name: str) -> str:
    """Normalize product name by stripping variant suffixes."""
    if pd.isna(product_name):
        return ""
    
    name = str(product_name).strip()
    colors = [
        'Black', 'White', 'Red', 'Blue', 'Green', 'Yellow', 'Orange', 'Pink', 'Purple',
        'Brown', 'Grey', 'Gray', 'Silver', 'Gold', 'Navy', 'Teal', 'Cyan', 'Magenta',
        'Beige', 'Tan', 'Maroon', 'Olive', 'Lime', 'Aqua', 'Coral', 'Salmon', 'Khaki',
        'Burgundy', 'Charcoal', 'Cream', 'Ivory', 'Mint', 'Peach', 'Turquoise', 'Violet',
        'Amber', 'Bronze', 'Copper', 'Indigo', 'Lavender', 'Mauve', 'Mustard', 'Plum',
        'Rose', 'Ruby', 'Sage', 'Scarlet', 'Taupe', 'Wine', 'Azure', 'Champagne'
    ]
    
    sizes = [
        'Small', 'Medium', 'Large', 'XLarge', 'XSmall', 'XL', 'XXL', 'S', 'M', 'L', 'XS', 'XXS',
        'Extra Small', 'Extra Large', '2XL', '3XL', '4XL', '5XL', 'XXXL', 'XXXXL',
        'Petite', 'Regular', 'Tall', 'Short', 'Plus', 'Oversized'
    ]
    
    size_pattern = r'\s*[-(\s]*(' + '|'.join(re.escape(s) for s in sizes) + r')(\s*[/)]\s*[^,]+)?\s*[)\s]*$'
    color_pattern = r'\s*[-(\s]*(' + '|'.join(re.escape(c) for c in colors) + r')(\s*[/)]\s*[^,]+)?\s*[)\s]*$'
    number_pattern = r'\s*[-(\s]*(\d+\.?\d*)\s*[)\s]*$'
    size_color_pattern = r'\s*-\s*(' + '|'.join(re.escape(s) for s in sizes) + r')\s*/\s*(' + '|'.join(re.escape(c) for c in colors) + r')\s*$'
    color_size_pattern = r'\s*-\s*(' + '|'.join(re.escape(c) for c in colors) + r')\s*/\s*(' + '|'.join(re.escape(s) for s in sizes) + r')\s*$'
    word_pattern = r'\s*-\s*[A-Z][a-z]+(\s*/\s*[A-Z][a-z]+)?\s*$'
    paren_pattern = r'\s*\([^)]+\)\s*$'
    
    previous = None
    while previous != name:
        previous = name
        name = re.sub(size_pattern, '', name, flags=re.IGNORECASE)
        name = re.sub(color_pattern, '', name, flags=re.IGNORECASE)
        name = re.sub(number_pattern, '', name)
        name = re.sub(size_color_pattern, '', name, flags=re.IGNORECASE)
        name = re.sub(color_size_pattern, '', name, flags=re.IGNORECASE)
        name = re.sub(word_pattern, '', name)
        name = re.sub(paren_pattern, '', name)
    
    name = name.strip().rstrip('-').strip().rstrip('(').strip()
    return name

def main():
    print("="*80)
    print("FINAL PRODUCT COUNT VERIFICATION")
    print("="*80)
    
    # Count by Handle (Shopify unique identifier)
    batch_files = [
        'data/output/shopify_products_batch_1_of_5.csv',
        'data/output/shopify_products_batch_2_of_5.csv',
        'data/output/shopify_products_batch_3_of_5.csv',
        'data/output/shopify_products_batch_4_of_5.csv',
        'data/output/shopify_products_batch_5_of_5.csv',
    ]
    
    all_handles = set()
    all_titles = set()
    all_base_names = set()
    
    print("\n1. COUNTING BY HANDLE (Shopify's Unique Identifier):")
    print("-" * 80)
    for batch_num, batch_file in enumerate(batch_files, 1):
        if not Path(batch_file).exists():
            continue
        df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
        parent_rows = df[df['Title'].astype(str).str.strip() != '']
        handles = {h for h in parent_rows['Handle'].astype(str).str.strip().unique() if h and h.lower() != 'nan'}
        titles = {t for t in parent_rows['Title'].astype(str).str.strip().unique() if t and t.lower() != 'nan'}
        base_names = {normalize_product_name(t) for t in titles if normalize_product_name(t)}
        
        all_handles.update(handles)
        all_titles.update(titles)
        all_base_names.update(base_names)
        
        print(f"   Batch {batch_num}: {len(handles):,} products")
    
    print(f"\n   ✅ TOTAL UNIQUE PRODUCTS (by Handle): {len(all_handles):,}")
    print(f"   ✅ TOTAL UNIQUE PRODUCTS (by Title): {len(all_titles):,}")
    print(f"   ✅ TOTAL UNIQUE BASE PRODUCTS (variants grouped): {len(all_base_names):,}")
    
    # Count source
    source_file = "/home/yuvraj/Documents/products.csv"
    print("\n2. COUNTING SOURCE FILE:")
    print("-" * 80)
    
    if Path(source_file).exists():
        df = pd.read_csv(source_file, dtype=str, keep_default_na=False, low_memory=False)
        name_column = 'Name' if 'Name' in df.columns else df.columns[0]
        product_names = df[name_column].astype(str).str.strip()
        product_names = product_names[product_names != '']
        product_names = product_names[product_names.str.lower() != 'nan']
        
        source_unique = len(set(product_names.unique()))
        source_base = len({normalize_product_name(n) for n in product_names.unique() if normalize_product_name(n)})
        
        print(f"   Total rows: {len(product_names):,}")
        print(f"   Unique product names: {source_unique:,}")
        print(f"   Unique BASE products (variants grouped): {source_base:,}")
        
        print("\n3. FINAL SUMMARY:")
        print("=" * 80)
        print(f"   Source: {source_unique:,} unique products ({source_base:,} base products)")
        print(f"   Migrated: {len(all_handles):,} unique products ({len(all_base_names):,} base products)")
        print(f"   Missing: {source_unique - len(all_handles):,} products ({source_base - len(all_base_names):,} base products)")
        print(f"   Coverage: {(len(all_handles) / source_unique * 100):.2f}% (by Handle)")
        print(f"   Coverage: {(len(all_base_names) / source_base * 100):.2f}% (by Base Name)")
    else:
        print(f"   ❌ Source file not found: {source_file}")
    
    print("\n" + "="*80)
    print("CONFIRMED: Migrated products =", len(all_handles))

if __name__ == "__main__":
    main()

