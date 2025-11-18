#!/usr/bin/env python3
"""
Migration Script for All Products
Migrates all products from source CSV to Shopify format.
"""

import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv
import yaml
import os
from loguru import logger
from colorama import init, Fore, Style
from datetime import datetime

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
        description='Migrate all products from source CSV to Shopify format'
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
        '--confirm',
        action='store_true',
        help='Skip confirmation prompt'
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
    log_file = config.get('logging', {}).get('log_file', './logs/migration_{date}.log')
    log_file = log_file.replace('{date}', datetime.now().strftime('%Y%m%d_%H%M%S'))
    
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level=log_level
    )
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        level=log_level,
        rotation="10 MB"
    )
    
    # Create output path
    output_path = Path(output_dir) / "shopify_products_all.csv"
    
    print(Fore.CYAN + "=" * 60)
    print(Fore.CYAN + "SHOPIFY PRODUCT MIGRATION - FULL MIGRATION")
    print(Fore.CYAN + "=" * 60)
    print(f"Source CSV: {source_csv}")
    print(f"Shopify Template: {shopify_template}")
    print(f"Output: {output_path}")
    print(f"Log file: {log_file}")
    print(Fore.CYAN + "=" * 60)
    print()
    
    # Confirmation prompt
    if not args.confirm:
        print(Fore.YELLOW + "⚠ WARNING: This will migrate ALL products from the source CSV.")
        print(Fore.YELLOW + "Make sure you have:")
        print("  1. Tested with migrate_10_products.py first")
        print("  2. Verified the field mapping is correct")
        print("  3. Backed up your source data")
        print()
        response = input(Fore.YELLOW + "Do you want to continue? (yes/no): ").strip().lower()
        if response not in ['yes', 'y']:
            print(Fore.RED + "Migration cancelled.")
            sys.exit(0)
        print()
    
    try:
        # Initialize orchestrator
        orchestrator = MigrationOrchestrator(
            source_csv_path=source_csv,
            shopify_template_path=shopify_template,
            output_path=str(output_path),
            mapping_config_path=mapping_config,
            sample_size=None  # No sample size for full migration
        )
        
        # Analyze source
        analysis = orchestrator.analyze_source()
        print(Fore.GREEN + f"✓ Source CSV analyzed: {analysis['row_count']} rows, {analysis['column_count']} columns")
        print()
        
        # Execute migration
        print(Fore.YELLOW + "Starting migration...")
        print(Fore.YELLOW + "This may take a while for large files. Please wait...")
        print()
        
        result = orchestrator.migrate()
        
        # Print results
        print()
        print(Fore.GREEN + "=" * 60)
        print(Fore.GREEN + "MIGRATION COMPLETED!")
        print(Fore.GREEN + "=" * 60)
        stats = result['statistics']
        validation = result['validation_report']
        
        print(f"Total rows processed: {stats['total_rows']}")
        print(f"Successfully migrated: {Fore.GREEN + str(stats['successful_rows'])}")
        print(f"Failed rows: {Fore.RED + str(stats['failed_rows'])}")
        error_rate = validation.get("error_rate", 0)
        print(f"Error rate: {Fore.YELLOW}{error_rate:.2f}%")
        print(f"Warnings: {Fore.YELLOW + str(len(stats['warnings']))}")
        print(f"Output file: {Fore.CYAN + result['output_file']}")
        print(f"Log file: {Fore.CYAN + log_file}")
        
        if stats['failed_rows'] > 0:
            print()
            print(Fore.YELLOW + f"⚠ {stats['failed_rows']} rows failed. Check error report for details.")
            error_report = Path(output_dir) / f"error_report_*.csv"
            print(f"Error report location: {error_report}")
        
        if validation.get('duplicate_skus'):
            print()
            print(Fore.YELLOW + f"⚠ Found {len(validation['duplicate_skus'])} duplicate SKUs. Review before importing to Shopify.")
        
        print()
        print(Fore.GREEN + "✓ Migration complete! You can now import the output CSV to Shopify.")
        
    except KeyboardInterrupt:
        print()
        print(Fore.YELLOW + "\n⚠ Migration interrupted by user.")
        sys.exit(1)
    except Exception as e:
        logger.exception("Migration failed with error")
        print(Fore.RED + f"\nERROR: Migration failed: {e}")
        print(Fore.RED + f"Check the log file for details: {log_file}")
        sys.exit(1)


if __name__ == '__main__':
    main()

