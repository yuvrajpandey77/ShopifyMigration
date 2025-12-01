#!/usr/bin/env python3
"""
Compare migrated product names with source CSV to verify completeness.
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


def compare_products():
    """Compare migrated products with source."""
    config = load_config()
    source_csv = (
        config.get('files', {}).get('source_csv') or
        '/home/yuvraj/Documents/wc-product-export-28-11-2025-1764343496895.csv'
    )
    
    migrated_file = "data/output/1926_product_names.csv"
    
    # Read migrated products
    handler = CSVHandler()
    migrated_df = pd.read_csv(migrated_file)
    migrated_names = set(migrated_df['Product Name'].str.strip().str.lower())
    
    # Read source CSV
    source_df = handler.read_csv(source_csv, low_memory=False)
    name_column = 'Name' if 'Name' in source_df.columns else source_df.columns[0]
    
    # Compute product groups from source
    source_df['__BaseName'] = source_df[name_column].apply(normalize_product_name)
    source_df['__ProductGroupID'] = source_df.apply(determine_product_group_id, axis=1)
    
    # Get one representative name per product group from source
    source_product_names = []
    for group_id, group_df in source_df.groupby('__ProductGroupID', sort=False):
        first_name = group_df[name_column].iloc[0]
        if pd.notna(first_name) and str(first_name).strip() and str(first_name).lower() != 'nan':
            source_product_names.append(str(first_name).strip())
    
    source_names = set(name.lower() for name in source_product_names)
    
    # Compare
    missing_in_migrated = source_names - migrated_names
    extra_in_migrated = migrated_names - source_names
    
    print(f"Source product groups: {len(source_names):,}")
    print(f"Migrated product names: {len(migrated_names):,}")
    print(f"Missing in migrated: {len(missing_in_migrated):,}")
    print(f"Extra in migrated: {len(extra_in_migrated):,}")
    
    if missing_in_migrated:
        print(f"\n❌ MISSING PRODUCTS ({len(missing_in_migrated)}):")
        for name in sorted(missing_in_migrated)[:20]:
            # Find original case
            original = [n for n in source_product_names if n.lower() == name][0]
            print(f"  - {original}")
        if len(missing_in_migrated) > 20:
            print(f"  ... and {len(missing_in_migrated) - 20} more")
    
    if not missing_in_migrated and not extra_in_migrated:
        print(f"\n✅ ALL PRODUCTS ACCOUNTED FOR - PERFECT MATCH!")
    elif len(missing_in_migrated) == 0:
        print(f"\n⚠️  All source products present, but {len(extra_in_migrated)} extra in migrated")
    else:
        print(f"\n❌ INCOMPLETE: {len(missing_in_migrated)} products missing from migrated list")


if __name__ == '__main__':
    compare_products()

