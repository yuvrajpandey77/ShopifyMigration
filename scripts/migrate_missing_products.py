#!/usr/bin/env python3
"""
Migrate missing products and create CSV with batch 1 field structure.
"""

import sys
import pandas as pd
from pathlib import Path
import yaml
from colorama import init, Fore

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.migration import MigrationOrchestrator, normalize_product_name, determine_product_group_id
from src.csv_handler import CSVHandler

init(autoreset=True)


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        return {}


def main():
    """Main function to migrate missing products."""
    config = load_config()
    
    missing_file = 'data/output/products_missing_in_uploaded.csv'
    batch1_file = 'data/output/shopify_products_batch_1_of_20.csv'
    source_csv = '/home/yuvraj/Documents/wc-product-export-28-11-2025-1764343496895.csv'
    shopify_template = config.get('files', {}).get('shopify_template') or '/home/yuvraj/Documents/product_template_csv_unit_price.csv'
    mapping_config = config.get('files', {}).get('mapping_config') or 'config/field_mapping.json'
    output_file = 'data/output/missing_products_with_batch1_fields.csv'
    
    print(Fore.CYAN + "="*80)
    print(Fore.CYAN + "MIGRATING MISSING PRODUCTS WITH BATCH 1 FIELD STRUCTURE")
    print(Fore.CYAN + "="*80)
    print()
    
    handler = CSVHandler()
    
    # Read missing products
    print(f"Reading missing products from: {missing_file}")
    missing_df = pd.read_csv(missing_file)
    missing_names = set(missing_df['Product Name'].str.strip().str.lower())
    print(f"Missing products: {Fore.GREEN + f'{len(missing_names):,}'}")
    print()
    
    # Read batch 1 to verify structure
    print(f"Reading batch 1 structure from: {batch1_file}")
    batch1_df = handler.read_csv(batch1_file, low_memory=False)
    print(f"Batch 1 columns: {len(batch1_df.columns)}")
    print(f"Batch 1 rows: {len(batch1_df):,}")
    print()
    
    # Read source CSV
    print(f"Reading source CSV: {source_csv}")
    source_df = handler.read_csv(source_csv, low_memory=False)
    name_column = 'Name'
    print(f"Source rows: {len(source_df):,}")
    print()
    
    # Get product groups from source
    print("Identifying missing products in source...")
    source_df['__BaseName'] = source_df[name_column].apply(normalize_product_name)
    source_df['__ProductGroupID'] = source_df.apply(determine_product_group_id, axis=1)
    
    # Filter source to only include missing products
    missing_product_rows = []
    for group_id, group_df in source_df.groupby('__ProductGroupID', sort=False):
        first_name = group_df[name_column].iloc[0]
        if pd.notna(first_name) and str(first_name).strip() and str(first_name).lower() != 'nan':
            name_str = str(first_name).strip()
            if name_str.lower() in missing_names:
                missing_product_rows.append(group_df)
    
    if not missing_product_rows:
        print(Fore.RED + "❌ No missing products found in source CSV!")
        return
    
    missing_source_df = pd.concat(missing_product_rows).sort_index()
    
    # Remove helper columns
    for col in ['__BaseName', '__ProductGroupID']:
        if col in missing_source_df.columns:
            missing_source_df = missing_source_df.drop(columns=[col])
    
    print(f"Found {len(missing_source_df):,} rows for missing products")
    print()
    
    # Save filtered source to temp file
    temp_source_file = Path('data/output/temp_missing_products_source.csv')
    temp_source_file.parent.mkdir(parents=True, exist_ok=True)
    handler.write_csv(missing_source_df, str(temp_source_file))
    print(f"Saved filtered source to: {temp_source_file}")
    print()
    
    # Migrate using MigrationOrchestrator
    print(Fore.YELLOW + "Migrating missing products...")
    print()
    
    try:
        orchestrator = MigrationOrchestrator(
            source_csv_path=str(temp_source_file),
            shopify_template_path=shopify_template,
            output_path=output_file,
            mapping_config_path=mapping_config,
            sample_size=None  # Process all
        )
        
        # Execute migration
        result = orchestrator.migrate()
        
        stats = result['statistics']
        print()
        print(Fore.GREEN + "="*80)
        print(Fore.GREEN + "MIGRATION COMPLETED!")
        print(Fore.GREEN + "="*80)
        successful_str = f"{stats['successful_rows']:,}"
        failed_str = f"{stats['failed_rows']:,}"
        print(f"Successfully migrated: {Fore.GREEN + successful_str}")
        print(f"Failed rows: {Fore.RED + failed_str}")
        print(f"Output file: {Fore.CYAN + output_file}")
        print()
        
        # Verify output has same structure as batch 1
        output_df = handler.read_csv(output_file, low_memory=False)
        print(f"Output rows: {len(output_df):,}")
        print(f"Output columns: {len(output_df.columns)}")
        print(f"Batch 1 columns: {len(batch1_df.columns)}")
        
        if set(output_df.columns) == set(batch1_df.columns):
            print(Fore.GREEN + "✅ Output has same structure as batch 1!")
        else:
            missing_cols = set(batch1_df.columns) - set(output_df.columns)
            extra_cols = set(output_df.columns) - set(batch1_df.columns)
            if missing_cols:
                print(Fore.YELLOW + f"⚠️  Missing columns: {missing_cols}")
            if extra_cols:
                print(Fore.YELLOW + f"⚠️  Extra columns: {extra_cols}")
        
        print()
        
    except Exception as e:
        print(Fore.RED + f"❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup temp file
        if temp_source_file.exists():
            temp_source_file.unlink()
            print(f"Cleaned up temp file: {temp_source_file}")


if __name__ == '__main__':
    main()

