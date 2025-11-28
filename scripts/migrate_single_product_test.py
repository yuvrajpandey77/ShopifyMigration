#!/usr/bin/env python3
"""
Single Product Migration Test Script
Tests migration of a single product to ensure all products can be migrated
even with missing images, prices, or descriptions.
"""

import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv
import yaml
import os
from loguru import logger
from colorama import init, Fore, Style
import pandas as pd

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.migration import MigrationOrchestrator
from src.csv_handler import CSVHandler

# Initialize colorama for colored output
init(autoreset=True)


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning(f"Config file not found: {config_path}")
        return {}


def get_file_paths(args, config: dict) -> tuple:
    """Get file paths from args, env vars, or config."""
    source_csv = (
        args.source or
        os.getenv('SOURCE_CSV_PATH') or
        config.get('files', {}).get('source_csv') or
        None
    )
    
    shopify_template = (
        args.template or
        os.getenv('SHOPIFY_TEMPLATE_PATH') or
        config.get('files', {}).get('shopify_template') or
        None
    )
    
    output_dir = (
        args.output or
        os.getenv('OUTPUT_DIR') or
        config.get('files', {}).get('output_dir') or
        './data/output'
    )
    
    mapping_config = (
        args.mapping or
        config.get('files', {}).get('mapping_config') or
        'config/field_mapping.json'
    )
    
    return source_csv, shopify_template, output_dir, mapping_config


def analyze_product_fields(row: pd.Series) -> dict:
    """Analyze a product row to identify missing fields."""
    analysis = {
        'has_name': False,
        'has_sku': False,
        'has_price': False,
        'has_image': False,
        'has_description': False,
        'missing_fields': []
    }
    
    # Check Name
    name = row.get('Name', '')
    if pd.notna(name) and str(name).strip() != '':
        analysis['has_name'] = True
    else:
        analysis['missing_fields'].append('Name')
    
    # Check SKU
    sku = row.get('SKU', '')
    if pd.notna(sku) and str(sku).strip() != '':
        analysis['has_sku'] = True
    else:
        analysis['missing_fields'].append('SKU')
    
    # Check Price
    regular_price = row.get('Regular price', '')
    sale_price = row.get('Sale price', '')
    has_price = False
    if pd.notna(regular_price):
        try:
            price_val = float(str(regular_price).replace('$', '').replace(',', '').strip())
            if price_val > 0:
                has_price = True
        except:
            pass
    if not has_price and pd.notna(sale_price):
        try:
            price_val = float(str(sale_price).replace('$', '').replace(',', '').strip())
            if price_val > 0:
                has_price = True
        except:
            pass
    if has_price:
        analysis['has_price'] = True
    else:
        analysis['missing_fields'].append('Price')
    
    # Check Image
    images = row.get('Images', '')
    if pd.notna(images) and str(images).strip() != '' and str(images).strip().lower() != 'nan':
        analysis['has_image'] = True
    else:
        analysis['missing_fields'].append('Image')
    
    # Check Description
    description = row.get('Description', '')
    if pd.notna(description) and str(description).strip() != '' and str(description).strip().lower() != 'nan':
        analysis['has_description'] = True
    else:
        analysis['missing_fields'].append('Description')
    
    return analysis


def main():
    """Main migration function."""
    parser = argparse.ArgumentParser(
        description='Test migration of a single product to Shopify format'
    )
    parser.add_argument(
        '--source',
        type=str,
        help='Path to source products CSV file'
    )
    parser.add_argument(
        '--template',
        type=str,
        help='Path to Shopify template CSV file'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Output directory for migrated CSV'
    )
    parser.add_argument(
        '--mapping',
        type=str,
        help='Path to field mapping configuration JSON'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yaml',
        help='Path to configuration YAML file'
    )
    parser.add_argument(
        '--row-index',
        type=int,
        default=0,
        help='Row index to migrate (default: 0)'
    )
    parser.add_argument(
        '--product-name',
        type=str,
        help='Product name to find and migrate'
    )
    
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Load configuration
    config = load_config(args.config)
    
    # Get file paths
    source_csv, shopify_template, output_dir, mapping_config = get_file_paths(args, config)
    
    # Validate required files
    if not source_csv:
        print(Fore.RED + "ERROR: Source CSV path not provided!")
        print("Please provide --source argument, set SOURCE_CSV_PATH env var, or configure in config.yaml")
        sys.exit(1)
    
    if not shopify_template:
        print(Fore.RED + "ERROR: Shopify template path not provided!")
        print("Please provide --template argument, set SHOPIFY_TEMPLATE_PATH env var, or configure in config.yaml")
        sys.exit(1)
    
    # Check if files exist
    if not Path(source_csv).exists():
        print(Fore.RED + f"ERROR: Source CSV file not found: {source_csv}")
        sys.exit(1)
    
    if not Path(shopify_template).exists():
        print(Fore.RED + f"ERROR: Shopify template file not found: {shopify_template}")
        sys.exit(1)
    
    # Setup logging
    log_level = config.get('logging', {}).get('level', 'INFO')
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level=log_level
    )
    
    # Load source CSV to find product
    handler = CSVHandler()
    source_df = handler.read_csv(source_csv, low_memory=False)
    
    # Find the product to migrate
    if args.product_name:
        # Find by product name
        matching_rows = source_df[source_df['Name'].str.contains(args.product_name, case=False, na=False)]
        if len(matching_rows) == 0:
            print(Fore.RED + f"ERROR: No product found with name containing: {args.product_name}")
            sys.exit(1)
        row_index = matching_rows.index[0]
        product_row = matching_rows.iloc[0]
    else:
        # Use row index
        if args.row_index >= len(source_df):
            print(Fore.RED + f"ERROR: Row index {args.row_index} is out of range. CSV has {len(source_df)} rows.")
            sys.exit(1)
        row_index = args.row_index
        product_row = source_df.iloc[row_index]
    
    # Analyze product fields
    field_analysis = analyze_product_fields(product_row)
    
    print(Fore.CYAN + "=" * 80)
    print(Fore.CYAN + "SINGLE PRODUCT MIGRATION TEST")
    print(Fore.CYAN + "=" * 80)
    print(f"Source CSV: {source_csv}")
    print(f"Shopify Template: {shopify_template}")
    print(f"Row Index: {row_index}")
    print(f"Product Name: {product_row.get('Name', 'N/A')}")
    print(f"SKU: {product_row.get('SKU', 'N/A')}")
    print()
    
    print(Fore.YELLOW + "FIELD ANALYSIS:")
    print(f"  Name: {'✓' if field_analysis['has_name'] else '✗'}")
    print(f"  SKU: {'✓' if field_analysis['has_sku'] else '✗'}")
    print(f"  Price: {'✓' if field_analysis['has_price'] else '✗'}")
    print(f"  Image: {'✓' if field_analysis['has_image'] else '✗'}")
    print(f"  Description: {'✓' if field_analysis['has_description'] else '✗'}")
    print()
    
    if field_analysis['missing_fields']:
        print(Fore.YELLOW + f"Missing Fields: {', '.join(field_analysis['missing_fields'])}")
        print(Fore.GREEN + "Note: Product will be migrated with empty/default values for missing fields")
        print()
    
    # Create a temporary CSV with just this product
    temp_csv = Path(output_dir) / "temp_single_product_test.csv"
    temp_csv.parent.mkdir(parents=True, exist_ok=True)
    
    # Get all rows that belong to the same product group (for variants)
    from src.migration import normalize_product_name, determine_product_group_id
    
    product_group_id = determine_product_group_id(product_row)
    source_df['__ProductGroupID'] = source_df.apply(determine_product_group_id, axis=1)
    product_group_df = source_df[source_df['__ProductGroupID'] == product_group_id].copy()
    
    # Save to temp CSV
    handler.write_csv(product_group_df.drop(columns=['__ProductGroupID']), str(temp_csv))
    
    print(Fore.CYAN + f"Created temporary CSV with {len(product_group_df)} rows (including variants)")
    print()
    
    # Create output path
    output_path = Path(output_dir) / "shopify_single_product_test.csv"
    
    try:
        # Initialize orchestrator with temp CSV
        orchestrator = MigrationOrchestrator(
            source_csv_path=str(temp_csv),
            shopify_template_path=shopify_template,
            output_path=str(output_path),
            mapping_config_path=mapping_config,
            sample_size=None  # Migrate all rows in the temp CSV
        )
        
        # Execute migration
        print(Fore.YELLOW + "Starting migration...")
        result = orchestrator.migrate()
        
        # Print results
        print()
        print(Fore.GREEN + "=" * 80)
        print(Fore.GREEN + "MIGRATION TEST COMPLETED!")
        print(Fore.GREEN + "=" * 80)
        stats = result['statistics']
        print(f"Total rows processed: {stats['total_rows']}")
        print(f"Successfully migrated: {Fore.GREEN + str(stats['successful_rows'])}")
        print(f"Failed rows: {Fore.RED + str(stats['failed_rows'])}")
        print(f"Output file: {Fore.CYAN + result['output_file']}")
        
        if stats['failed_rows'] > 0:
            print()
            print(Fore.RED + f"✗ {stats['failed_rows']} rows failed. Check error report for details.")
            if stats.get('errors'):
                for error in stats['errors'][:5]:
                    print(Fore.RED + f"  - {error}")
        else:
            print()
            print(Fore.GREEN + "✓ All rows migrated successfully!")
            print()
            print(Fore.CYAN + "Next steps:")
            print("1. Review the output CSV file to verify the migration")
            print("2. Check that missing fields are handled correctly (empty/default values)")
            print("3. If everything looks good, proceed with full batch migration")
        
        # Clean up temp file
        if temp_csv.exists():
            temp_csv.unlink()
            print()
            print(Fore.GREEN + f"✓ Cleaned up temporary file: {temp_csv}")
        
    except Exception as e:
        logger.exception("Migration failed with error")
        print(Fore.RED + f"\nERROR: Migration failed: {e}")
        # Clean up temp file on error
        if temp_csv.exists():
            temp_csv.unlink()
        sys.exit(1)


if __name__ == '__main__':
    main()

