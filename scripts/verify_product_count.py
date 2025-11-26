import pandas as pd
import sys
from pathlib import Path

def count_unique_products_by_handle():
    """Count unique products by Handle across all batches (Handle is Shopify's unique identifier)."""
    batch_files = [
        'data/output/shopify_products_batch_1_of_5.csv',
        'data/output/shopify_products_batch_2_of_5.csv',
        'data/output/shopify_products_batch_3_of_5.csv',
        'data/output/shopify_products_batch_4_of_5.csv',
        'data/output/shopify_products_batch_5_of_5.csv',
    ]
    
    all_handles = set()
    batch_handles = {}
    duplicate_handles = set()
    
    print("="*80)
    print("COUNTING UNIQUE PRODUCTS BY HANDLE (Shopify's Unique Identifier)")
    print("="*80)
    
    for batch_num, batch_file in enumerate(batch_files, 1):
        if not Path(batch_file).exists():
            print(f"⚠️  Warning: {batch_file} not found, skipping...")
            continue
            
        print(f"\nReading Batch {batch_num}...")
        df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
        
        # Get unique handles (only from parent rows - rows with non-empty Title)
        parent_rows = df[df['Title'].astype(str).str.strip() != '']
        handles = set(parent_rows['Handle'].astype(str).str.strip().unique())
        
        # Remove empty handles
        handles = {h for h in handles if h and h.lower() != 'nan'}
        
        batch_handles[batch_num] = handles
        
        # Check for duplicates
        duplicates = handles & all_handles
        if duplicates:
            duplicate_handles.update(duplicates)
            print(f"  ⚠️  Found {len(duplicates)} duplicate handles with previous batches!")
        
        all_handles.update(handles)
        print(f"  Unique handles in this batch: {len(handles):,}")
        print(f"  Total unique handles so far: {len(all_handles):,}")
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total unique products (by Handle) across all batches: {len(all_handles):,}")
    
    if duplicate_handles:
        print(f"\n⚠️  WARNING: Found {len(duplicate_handles)} handles that appear in multiple batches!")
        print("First 10 duplicate handles:")
        for handle in list(duplicate_handles)[:10]:
            print(f"  - {handle}")
    else:
        print("\n✅ No duplicate handles found across batches")
    
    return len(all_handles), batch_handles, duplicate_handles

def count_products_by_title():
    """Count unique products by Title (parent rows only)."""
    batch_files = [
        'data/output/shopify_products_batch_1_of_5.csv',
        'data/output/shopify_products_batch_2_of_5.csv',
        'data/output/shopify_products_batch_3_of_5.csv',
        'data/output/shopify_products_batch_4_of_5.csv',
        'data/output/shopify_products_batch_5_of_5.csv',
    ]
    
    all_titles = set()
    batch_titles = {}
    
    print("\n" + "="*80)
    print("COUNTING UNIQUE PRODUCTS BY TITLE (Parent Rows Only)")
    print("="*80)
    
    for batch_num, batch_file in enumerate(batch_files, 1):
        if not Path(batch_file).exists():
            continue
            
        df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
        
        # Get unique titles (only from parent rows)
        parent_rows = df[df['Title'].astype(str).str.strip() != '']
        titles = set(parent_rows['Title'].astype(str).str.strip().unique())
        
        # Remove empty strings
        titles = {t for t in titles if t and t.lower() != 'nan'}
        
        batch_titles[batch_num] = titles
        all_titles.update(titles)
        print(f"Batch {batch_num}: {len(titles):,} unique titles")
    
    print(f"\nTotal unique products (by Title) across all batches: {len(all_titles):,}")
    
    return len(all_titles), batch_titles

def count_source_products():
    """Count unique products in source file."""
    source_file = "/home/yuvraj/Documents/products.csv"
    
    print("\n" + "="*80)
    print("COUNTING PRODUCTS IN SOURCE FILE")
    print("="*80)
    
    if not Path(source_file).exists():
        print(f"❌ ERROR: Source file not found: {source_file}")
        return 0, 0
    
    try:
        df = pd.read_csv(source_file, dtype=str, keep_default_na=False, low_memory=False)
    except Exception as e:
        print(f"❌ Error reading source file: {e}")
        return 0, 0
    
    # Find Name column
    possible_name_columns = ['Name', 'Product Name', 'Title', 'Product Title', 'name', 'product_name', 'title']
    name_column = None
    for col in possible_name_columns:
        if col in df.columns:
            name_column = col
            break
    
    if not name_column:
        name_column = df.columns[0]
    
    print(f"Using '{name_column}' column")
    
    # Count unique product names
    product_names = df[name_column].astype(str).str.strip()
    product_names = product_names[product_names != '']
    product_names = product_names[product_names.str.lower() != 'nan']
    
    unique_names = set(product_names.unique())
    total_rows = len(product_names)
    
    print(f"Total rows in source: {total_rows:,}")
    print(f"Unique product names: {len(unique_names):,}")
    
    return len(unique_names), total_rows

def main():
    print("="*80)
    print("COMPREHENSIVE PRODUCT COUNT VERIFICATION")
    print("="*80)
    
    # Count by Handle (most accurate - Shopify's unique identifier)
    unique_by_handle, batch_handles, duplicates = count_unique_products_by_handle()
    
    # Count by Title
    unique_by_title, batch_titles = count_products_by_title()
    
    # Count source
    source_unique, source_total = count_source_products()
    
    # Final summary
    print("\n" + "="*80)
    print("FINAL VERIFICATION SUMMARY")
    print("="*80)
    print(f"Source file:")
    print(f"  - Total rows: {source_total:,}")
    print(f"  - Unique product names: {source_unique:,}")
    print(f"\nMigrated batches:")
    print(f"  - Unique products by Handle: {unique_by_handle:,} ⭐ (MOST ACCURATE)")
    print(f"  - Unique products by Title: {unique_by_title:,}")
    
    if duplicates:
        print(f"\n⚠️  WARNING: {len(duplicates)} products appear in multiple batches (duplicates)")
        print(f"   Actual unique count might be: {unique_by_handle - len(duplicates):,}")
    
    print(f"\nMissing products:")
    print(f"  - By Handle count: {source_unique - unique_by_handle:,}")
    print(f"  - Coverage: {(unique_by_handle / source_unique * 100):.2f}%")
    
    # Per-batch breakdown
    print("\n" + "="*80)
    print("PER-BATCH BREAKDOWN (by Handle)")
    print("="*80)
    for batch_num, handles in sorted(batch_handles.items()):
        print(f"Batch {batch_num}: {len(handles):,} unique products")
    
    total_in_batches = sum(len(h) for h in batch_handles.values())
    print(f"\nSum of all batches: {total_in_batches:,}")
    print(f"Unique across all batches: {unique_by_handle:,}")
    if total_in_batches != unique_by_handle:
        print(f"⚠️  Difference: {total_in_batches - unique_by_handle:,} (duplicates across batches)")

if __name__ == "__main__":
    main()

