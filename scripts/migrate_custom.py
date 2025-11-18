#!/usr/bin/env python3
"""
Custom Migration Script
Migrates specified number of products with custom output filename.
"""

import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv
import yaml
import os
from loguru import logger
from colorama import init, Fore

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.migration import MigrationOrchestrator

# Initialize colorama
init(autoreset=True)


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning(f"Config file not found: {config_path}")
        return {}


def main():
    """Main migration function."""
    parser = argparse.ArgumentParser(
        description='Migrate products with custom output filename'
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
        required=True,
        help='Output CSV file path (e.g., data/output/custom_name.csv)'
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        required=True,
        help='Number of products to migrate'
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
    
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Load configuration
    config = load_config(args.config)
    
    # Get file paths
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
    
    mapping_config = (
        args.mapping or
        config.get('files', {}).get('mapping_config') or
        'config/field_mapping.json'
    )
    
    # Validate required files
    if not source_csv:
        print(Fore.RED + "ERROR: Source CSV path not provided!")
        sys.exit(1)
    
    if not shopify_template:
        print(Fore.RED + "ERROR: Shopify template path not provided!")
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
    
    # Create output path
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(Fore.CYAN + "=" * 60)
    print(Fore.CYAN + f"SHOPIFY PRODUCT MIGRATION - {args.sample_size} Products")
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
        
    except Exception as e:
        logger.exception("Migration failed with error")
        print(Fore.RED + f"\nERROR: Migration failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

