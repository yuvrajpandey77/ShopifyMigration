#!/usr/bin/env python3
"""
Extract all product names from source CSV and save to a simple CSV file.
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


def extract_all_product_names(source_csv_path: str, output_file: str = "data/output/all_product_names.csv"):
    """
    Extract all unique product names from source CSV.
    
    Args:
        source_csv_path: Path to source CSV file
        output_file: Path to output CSV file
    """
    print(Fore.CYAN + "="*80)
    print(Fore.CYAN + "EXTRACTING ALL PRODUCT NAMES")
    print(Fore.CYAN + "="*80)
    print()
    
    handler = CSVHandler()
    print(f"Reading source CSV: {source_csv_path}")
    df = handler.read_csv(source_csv_path, low_memory=False)
    
    print(f"Total rows in source: {Fore.GREEN + f'{len(df):,}'}")
    
    # Get Name column
    name_column = 'Name' if 'Name' in df.columns else df.columns[0]
    print(f"Using column: {Fore.CYAN + name_column}")
    
    # Get all product names
    all_names = df[name_column].astype(str).str.strip()
    all_names = all_names[all_names != '']
    all_names = all_names[all_names.str.lower() != 'nan']
    
    # Get unique names
    unique_names = set(all_names.unique())
    print(f"Unique product names: {Fore.GREEN + f'{len(unique_names):,}'}")
    
    # Also get base names (normalized)
    print("\nComputing base product names...")
    df['__BaseName'] = df[name_column].apply(normalize_product_name)
    df['__ProductGroupID'] = df.apply(determine_product_group_id, axis=1)
    
    # Get unique base names
    unique_base_names = set()
    for base_name in df['__BaseName'].unique():
        if base_name and str(base_name).strip() and str(base_name).lower() != 'nan':
            unique_base_names.add(base_name)
    
    print(f"Unique base products (variants grouped): {Fore.GREEN + f'{len(unique_base_names):,}'}")
    
    # Get product groups count
    product_groups = len(df['__ProductGroupID'].unique())
    print(f"Product groups: {Fore.GREEN + f'{product_groups:,}'}")
    
    # Create output DataFrame with all unique names
    output_df = pd.DataFrame({
        'Product Name': sorted(unique_names)
    })
    
    # Save to CSV
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_path, index=False)
    
    print()
    print(Fore.GREEN + f"✅ Saved {len(unique_names):,} product names to: {output_path}")
    print()
    
    # Also create a file with base names
    base_output_file = output_path.parent / "all_base_product_names.csv"
    base_output_df = pd.DataFrame({
        'Base Product Name': sorted(unique_base_names)
    })
    base_output_df.to_csv(base_output_file, index=False)
    
    print(Fore.GREEN + f"✅ Saved {len(unique_base_names):,} base product names to: {base_output_file}")
    print()
    
    # Show first few names
    print(Fore.CYAN + "First 10 product names:")
    for i, name in enumerate(sorted(unique_names)[:10], 1):
        print(f"  {i}. {name}")
    
    if len(unique_names) > 10:
        print(f"  ... and {len(unique_names) - 10} more")
    
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
        print(Fore.RED + f"❌ Source file not found: {source_csv}")
        print(Fore.YELLOW + "Trying alternative path...")
        source_csv = '/home/yuvraj/Documents/wc-product-export-28-11-2025-1764343496895.csv'
        if not Path(source_csv).exists():
            print(Fore.RED + f"❌ Source file not found: {source_csv}")
            sys.exit(1)
    
    output_file = "data/output/all_product_names.csv"
    extract_all_product_names(source_csv, output_file)


if __name__ == '__main__':
    main()

