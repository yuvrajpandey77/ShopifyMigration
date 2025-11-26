import pandas as pd
import sys
import re
from pathlib import Path

def normalize_product_name(product_name: str) -> str:
    """
    Normalize product name by stripping variant suffixes (size, color, numeric).
    This is the same function used in the migration script.
    """
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

def extract_base_products_from_batches():
    """Extract all unique BASE product names (grouping variants) from all 5 batch files."""
    batch_files = [
        'data/output/shopify_products_batch_1_of_5.csv',
        'data/output/shopify_products_batch_2_of_5.csv',
        'data/output/shopify_products_batch_3_of_5.csv',
        'data/output/shopify_products_batch_4_of_5.csv',
        'data/output/shopify_products_batch_5_of_5.csv',
    ]
    
    all_base_products = set()
    variant_counts = {}  # Track how many variants per base product
    
    print("="*80)
    print("EXTRACTING BASE PRODUCT NAMES FROM BATCH FILES (GROUPING VARIANTS)")
    print("="*80)
    
    for batch_file in batch_files:
        if not Path(batch_file).exists():
            print(f"⚠️  Warning: {batch_file} not found, skipping...")
            continue
            
        print(f"\nReading {batch_file}...")
        df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
        
        # Extract product names from Title column (only parent rows, not variants)
        # Parent rows have non-empty Title
        parent_rows = df[df['Title'].astype(str).str.strip() != '']
        product_names = parent_rows['Title'].astype(str).str.strip().unique()
        
        # Normalize to base names (group variants)
        base_names = set()
        for name in product_names:
            if name and name.lower() != 'nan':
                base_name = normalize_product_name(name)
                if base_name:
                    base_names.add(base_name)
                    # Count variants
                    if base_name not in variant_counts:
                        variant_counts[base_name] = 0
                    variant_counts[base_name] += 1
        
        batch_count = len(base_names)
        all_base_products.update(base_names)
        print(f"  Found {batch_count:,} unique BASE products (variants grouped)")
    
    print(f"\n✅ Total unique BASE products across all batches: {len(all_base_products):,}")
    return all_base_products, variant_counts

def extract_base_products_from_source(source_file):
    """Extract all unique BASE product names (grouping variants) from source file."""
    print("\n" + "="*80)
    print("EXTRACTING BASE PRODUCT NAMES FROM SOURCE FILE (GROUPING VARIANTS)")
    print("="*80)
    print(f"Reading {source_file}...")
    
    if not Path(source_file).exists():
        print(f"❌ ERROR: Source file not found: {source_file}")
        return set(), {}
    
    # Read source file - need to detect encoding and handle large files
    try:
        df = pd.read_csv(source_file, dtype=str, keep_default_na=False, low_memory=False)
    except Exception as e:
        print(f"❌ Error reading source file: {e}")
        return set(), {}
    
    # Check what column contains product names
    # Common column names: Name, Product Name, Title, Product Title, etc.
    possible_name_columns = ['Name', 'Product Name', 'Title', 'Product Title', 'name', 'product_name', 'title']
    
    name_column = None
    for col in possible_name_columns:
        if col in df.columns:
            name_column = col
            break
    
    if not name_column:
        print("⚠️  Could not find product name column. Available columns:")
        print(df.columns.tolist()[:20])  # Show first 20 columns
        # Try to use first column as fallback
        name_column = df.columns[0]
        print(f"Using '{name_column}' as product name column")
    else:
        print(f"✅ Using '{name_column}' column for product names")
    
    # Extract product names and normalize to base names
    product_names = df[name_column].astype(str).str.strip()
    # Remove empty strings
    product_names = product_names[product_names != '']
    product_names = product_names[product_names.str.lower() != 'nan']
    
    # Normalize to base names (group variants)
    base_products = {}
    variant_counts = {}
    
    for name in product_names:
        if name:
            base_name = normalize_product_name(name)
            if base_name:
                base_products[base_name] = base_products.get(base_name, [])
                base_products[base_name].append(name)
                variant_counts[base_name] = variant_counts.get(base_name, 0) + 1
    
    print(f"✅ Found {len(base_products):,} unique BASE products in source file (variants grouped)")
    print(f"   Total individual product entries: {len(product_names):,}")
    
    return set(base_products.keys()), variant_counts

def main():
    # Get source file path from config
    source_file = "/home/yuvraj/Documents/products.csv"
    
    # Extract BASE products from batches (variants grouped)
    batch_base_products, batch_variant_counts = extract_base_products_from_batches()
    
    # Extract BASE products from source (variants grouped)
    source_base_products, source_variant_counts = extract_base_products_from_source(source_file)
    
    if not source_base_products:
        print("\n❌ Could not extract products from source file. Exiting.")
        sys.exit(1)
    
    # Find missing BASE products
    missing_base_products = source_base_products - batch_base_products
    
    print("\n" + "="*80)
    print("COMPARISON RESULTS (BASE PRODUCTS - VARIANTS GROUPED)")
    print("="*80)
    print(f"Source BASE products: {len(source_base_products):,}")
    print(f"Migrated BASE products (in batches): {len(batch_base_products):,}")
    print(f"Missing BASE products: {len(missing_base_products):,}")
    
    # Calculate coverage percentage
    if source_base_products:
        coverage = (len(batch_base_products) / len(source_base_products)) * 100
        print(f"Coverage: {coverage:.2f}%")
    
    if missing_base_products:
        print("\n" + "="*80)
        print("MISSING BASE PRODUCTS (from source but not in batches)")
        print("="*80)
        
        # Sort missing products
        missing_sorted = sorted(missing_base_products)
        
        # Save to file
        output_file = "data/output/missing_base_products.txt"
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for i, product in enumerate(missing_sorted, 1):
                variant_count = source_variant_counts.get(product, 0)
                f.write(f"{i}. {product} ({variant_count} variant(s))\n")
        
        # Print first 50
        print(f"\nFirst 50 missing BASE products (full list saved to {output_file}):")
        for i, product in enumerate(missing_sorted[:50], 1):
            variant_count = source_variant_counts.get(product, 0)
            print(f"  {i}. {product} ({variant_count} variant(s))")
        
        if len(missing_sorted) > 50:
            print(f"\n... and {len(missing_sorted) - 50} more (see {output_file})")
        
        print(f"\n✅ Full list of {len(missing_sorted):,} missing BASE products saved to: {output_file}")
    else:
        print("\n✅ SUCCESS: All BASE products from source are present in the batches!")
    
    # Also save all migrated BASE products
    migrated_file = "data/output/migrated_base_products.txt"
    with open(migrated_file, 'w', encoding='utf-8') as f:
        for product in sorted(batch_base_products):
            variant_count = batch_variant_counts.get(product, 0)
            f.write(f"{product} ({variant_count} variant(s))\n")
    print(f"\n✅ List of {len(batch_base_products):,} migrated BASE products saved to: {migrated_file}")

if __name__ == "__main__":
    main()

