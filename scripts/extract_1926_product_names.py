#!/usr/bin/env python3
"""
Extract one product name per product group (1926 products).
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


def extract_product_group_names(source_csv_path: str, output_file: str = "data/output/1926_product_names.csv"):
    """
    Extract one representative name per product group (1926 products).
    
    Args:
        source_csv_path: Path to source CSV file
        output_file: Path to output CSV file
    """
    print(Fore.CYAN + "="*80)
    print(Fore.CYAN + "EXTRACTING 1926 PRODUCT GROUP NAMES")
    print(Fore.CYAN + "="*80)
    print()
    
    handler = CSVHandler()
    print(f"Reading source CSV: {source_csv_path}")
    df = handler.read_csv(source_csv_path, low_memory=False)
    
    print(f"Total rows in source: {Fore.GREEN + f'{len(df):,}'}")
    
    # Get Name column
    name_column = 'Name' if 'Name' in df.columns else df.columns[0]
    print(f"Using column: {Fore.CYAN + name_column}")
    
    # Compute product groups
    print("\nComputing product groups...")
    df['__BaseName'] = df[name_column].apply(normalize_product_name)
    df['__ProductGroupID'] = df.apply(determine_product_group_id, axis=1)
    
    # Get one representative name per product group
    product_names = []
    
    for group_id, group_df in df.groupby('__ProductGroupID', sort=False):
        # Get the first product name from this group as the representative name
        first_name = group_df[name_column].iloc[0]
        if pd.notna(first_name) and str(first_name).strip() and str(first_name).lower() != 'nan':
            product_names.append(str(first_name).strip())
    
    # Sort alphabetically
    product_names = sorted(set(product_names))
    
    print(f"Product groups found: {Fore.GREEN + f'{len(product_names):,}'}")
    
    # Create output DataFrame
    output_df = pd.DataFrame({
        'Product Name': product_names
    })
    
    # Save to CSV
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_path, index=False)
    
    print()
    print(Fore.GREEN + f"✅ Saved {len(product_names):,} product names to: {output_path}")
    print()
    
    # Show first few names
    print(Fore.CYAN + "First 20 product names:")
    for i, name in enumerate(product_names[:20], 1):
        print(f"  {i:3d}. {name}")
    
    if len(product_names) > 20:
        print(f"  ... and {len(product_names) - 20} more")
    
    print()
    return output_path


def main():
    """Main function."""
    config = load_config()
    source_csv = (
        config.get('files', {}).get('source_csv') or
        '/home/yuvraj/Documents/products.csv'
    )
    
    # Check if source file exists
    if not Path(source_csv).exists():
        print(Fore.YELLOW + "Trying alternative path...")
        source_csv = '/home/yuvraj/Documents/wc-product-export-28-11-2025-1764343496895.csv'
        if not Path(source_csv).exists():
            print(Fore.RED + f"❌ Source file not found: {source_csv}")
            sys.exit(1)
    
    output_file = "data/output/1926_product_names.csv"
    extract_product_group_names(source_csv, output_file)


if __name__ == '__main__':
    main()

