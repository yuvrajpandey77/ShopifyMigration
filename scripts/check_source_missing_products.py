#!/usr/bin/env python3
"""
Compare 1926_product_names.csv with source CSV to find products missing in source.
"""

import sys
import pandas as pd
from pathlib import Path
import yaml
from colorama import init, Fore

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.migration import normalize_product_name, determine_product_group_id
from src.csv_handler import CSVHandler

init(autoreset=True)


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        return {}


def find_missing_in_source():
    """Find products in 1926 file that are missing from source CSV."""
    config = load_config()
    source_csv = (
        config.get('files', {}).get('source_csv') or
        '/home/yuvraj/Documents/wc-product-export-28-11-2025-1764343496895.csv'
    )
    
    # Try the wc-product-export file first
    if not Path(source_csv).exists():
        source_csv = '/home/yuvraj/Documents/wc-product-export-28-11-2025-1764343496895.csv'
    
    migrated_file = "data/output/1926_product_names.csv"
    output_file = "data/output/products_missing_in_source.csv"
    
    print(Fore.CYAN + "="*80)
    print(Fore.CYAN + "CHECKING IF SOURCE CSV HAS ALL PRODUCTS FROM 1926 FILE")
    print(Fore.CYAN + "="*80)
    print()
    
    # Read 1926 product names (the complete list)
    print(f"Reading 1926 product names from: {migrated_file}")
    migrated_df = pd.read_csv(migrated_file)
    migrated_names = set(migrated_df['Product Name'].str.strip().str.lower())
    print(f"Total products in 1926 file: {Fore.GREEN + f'{len(migrated_names):,}'}")
    print()
    
    # Read source CSV
    print(f"Reading source CSV: {source_csv}")
    handler = CSVHandler()
    source_df = handler.read_csv(source_csv, low_memory=False)
    name_column = 'Name' if 'Name' in source_df.columns else source_df.columns[0]
    print(f"Total rows in source: {Fore.CYAN + f'{len(source_df):,}'}")
    print(f"Using column: {Fore.CYAN + name_column}")
    print()
    
    # Get all product names from source (exact matches and normalized)
    print("Extracting product names from source...")
    source_product_names = source_df[name_column].astype(str).str.strip()
    source_product_names = source_product_names[source_product_names != '']
    source_product_names = source_product_names[source_product_names.str.lower() != 'nan']
    
    # Create sets for comparison (both exact and normalized)
    source_names_exact = set(source_product_names.str.lower())
    
    # Also check normalized names
    source_df['__BaseName'] = source_df[name_column].apply(normalize_product_name)
    source_base_names = set(source_df['__BaseName'].str.strip().str.lower())
    source_base_names = {n for n in source_base_names if n and n != 'nan'}
    
    print(f"Unique product names in source: {Fore.CYAN + f'{len(source_names_exact):,}'}")
    print(f"Unique base product names in source: {Fore.CYAN + f'{len(source_base_names):,}'}")
    print()
    
    # Find missing products
    # First try exact match
    missing_exact = migrated_names - source_names_exact
    
    # Then try normalized match for remaining
    missing_after_normalized = set()
    for migrated_name in missing_exact:
        normalized = normalize_product_name(migrated_name)
        if normalized.lower() not in source_base_names:
            missing_after_normalized.add(migrated_name)
    
    missing_products = missing_after_normalized
    
    print(Fore.CYAN + "="*80)
    print(Fore.CYAN + "COMPARISON RESULTS")
    print(Fore.CYAN + "="*80)
    print(f"Products in 1926 file:        {Fore.GREEN + f'{len(migrated_names):,}'}")
    print(f"Products in source (exact):   {Fore.CYAN + f'{len(source_names_exact):,}'}")
    print(f"Products in source (base):    {Fore.CYAN + f'{len(source_base_names):,}'}")
    print(f"Missing in source:           {Fore.RED + f'{len(missing_products):,}'}")
    print()
    
    if missing_products:
        # Get original case names from 1926 file
        missing_original_case = []
        for name_lower in missing_products:
            # Find original case from migrated file
            original = migrated_df[migrated_df['Product Name'].str.strip().str.lower() == name_lower]
            if not original.empty:
                missing_original_case.append(original.iloc[0]['Product Name'].strip())
            else:
                missing_original_case.append(name_lower)
        
        missing_original_case = sorted(missing_original_case)
        
        # Create DataFrame with missing products
        missing_df = pd.DataFrame({
            'Product Name': missing_original_case
        })
        
        # Save to CSV
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        missing_df.to_csv(output_path, index=False)
        
        print(Fore.RED + f"❌ Found {len(missing_products)} products missing in source!")
        print()
        print(f"First 20 missing products:")
        for i, name in enumerate(missing_original_case[:20], 1):
            print(f"  {i:3d}. {name}")
        if len(missing_original_case) > 20:
            print(f"  ... and {len(missing_original_case) - 20} more")
        print()
        print(Fore.GREEN + f"✅ Saved missing products to: {output_path}")
        print(f"   Total missing products: {len(missing_products):,}")
    else:
        print(Fore.GREEN + "✅ ALL PRODUCTS FROM 1926 FILE ARE PRESENT IN SOURCE!")
        print()
        # Create empty file to indicate no missing products
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        empty_df = pd.DataFrame({'Product Name': []})
        empty_df.to_csv(output_path, index=False)
        print(f"Created empty file: {output_path}")
    
    print()


if __name__ == '__main__':
    find_missing_in_source()

