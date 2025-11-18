#!/usr/bin/env python3
"""
Migration Script for 10 Products (Testing)
Tests the migration process with a small sample before full migration.
"""

import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv
import yaml
import os
from loguru import logger
from colorama import init, Fore, Style

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.migration import MigrationOrchestrator

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
    # Priority: command line args > environment variables > config file
    
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


def main():
    """Main migration function."""
    parser = argparse.ArgumentParser(
        description='Migrate 10 products from source CSV to Shopify format (testing)'
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
        '--sample-size',
        type=int,
        default=10,
        help='Number of products to migrate (default: 10)'
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
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level=log_level
    )
    
    # Create output path
    output_path = Path(output_dir) / "shopify_products_10.csv"
    
    print(Fore.CYAN + "=" * 60)
    print(Fore.CYAN + "SHOPIFY PRODUCT MIGRATION - TEST MODE (10 Products)")
    print(Fore.CYAN + "=" * 60)
    print(f"Source CSV: {source_csv}")
    print(f"Shopify Template: {shopify_template}")
    print(f"Output: {output_path}")
    print(f"Sample Size: {args.sample_size}")
    print(Fore.CYAN + "=" * 60)
    print()
    
    try:
        # Initialize orchestrator
        orchestrator = MigrationOrchestrator(
            source_csv_path=source_csv,
            shopify_template_path=shopify_template,
            output_path=str(output_path),
            mapping_config_path=mapping_config,
            sample_size=args.sample_size
        )
        
        # Analyze source
        analysis = orchestrator.analyze_source()
        print(Fore.GREEN + f"✓ Source CSV analyzed: {analysis['row_count']} rows, {analysis['column_count']} columns")
        print()
        
        # Execute migration
        print(Fore.YELLOW + "Starting migration...")
        result = orchestrator.migrate()
        
        # Print results
        print()
        print(Fore.GREEN + "=" * 60)
        print(Fore.GREEN + "MIGRATION COMPLETED SUCCESSFULLY!")
        print(Fore.GREEN + "=" * 60)
        stats = result['statistics']
        print(f"Total rows processed: {stats['total_rows']}")
        print(f"Successfully migrated: {Fore.GREEN + str(stats['successful_rows'])}")
        print(f"Failed rows: {Fore.RED + str(stats['failed_rows'])}")
        print(f"Output file: {Fore.CYAN + result['output_file']}")
        
        if stats['failed_rows'] > 0:
            print()
            print(Fore.YELLOW + f"⚠ {stats['failed_rows']} rows failed. Check error report for details.")
        
        if stats['warnings']:
            print()
            print(Fore.YELLOW + f"⚠ {len(stats['warnings'])} warnings generated. Check logs for details.")
        
        print()
        print(Fore.CYAN + "Next steps:")
        print("1. Review the output CSV file")
        print("2. Verify all fields are correctly mapped")
        print("3. If everything looks good, run migrate_all_products.py")
        
    except Exception as e:
        logger.exception("Migration failed with error")
        print(Fore.RED + f"\nERROR: Migration failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

