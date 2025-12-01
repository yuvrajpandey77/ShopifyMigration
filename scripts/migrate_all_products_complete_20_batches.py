#!/usr/bin/env python3
"""
Complete Migration Script - 20 Batches - ALL PRODUCTS
Migrates ALL products from source CSV in 20 batches with no filtering.
Ensures 100% coverage of all products.
"""

import sys
import argparse
from pathlib import Path
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = lambda: None
import yaml
import os
import pandas as pd
from loguru import logger
from colorama import init, Fore
import math

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.migration import MigrationOrchestrator, normalize_product_name, determine_product_group_id
from src.csv_handler import CSVHandler

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


def split_source_csv_all_products(source_csv_path: str, num_batches: int = 20) -> list:
    """
    Split source CSV into batches without breaking product groups.
    INCLUDES ALL PRODUCTS - NO FILTERING.
    
    Args:
        source_csv_path: Path to source CSV file
        num_batches: Number of batches to create (default: 20)
        
    Returns:
        List of DataFrames, one for each batch
    """
    logger.info(f"Loading source CSV: {source_csv_path}")
    handler = CSVHandler()
    df = handler.read_csv(source_csv_path, low_memory=False)
    
    total_rows = len(df)
    logger.info(f"Total rows in source: {total_rows:,}")
    logger.info(f"Number of batches: {num_batches}")
    
    # Compute base names so we can keep full product groups together
    logger.info("Extracting base product names for grouping")
    df['__BaseName'] = df['Name'].apply(normalize_product_name)
    df['__ProductGroupID'] = df.apply(determine_product_group_id, axis=1)
    
    # Group ALL products (no filtering - include everything)
    valid_groups = []
    
    for group_id, group_df in df.groupby('__ProductGroupID', sort=False):
        first_idx = group_df.index.min()
        valid_groups.append((first_idx, group_id, group_df.copy()))
    
    valid_groups.sort(key=lambda item: item[0])
    
    total_groups = len(valid_groups)
    logger.info(f"Total product groups: {total_groups:,}")
    logger.info(f"✅ Including ALL products (no filtering by price/image/description)")
    
    # Count unique base products
    unique_base_names = set()
    for _, _, group_df in valid_groups:
        base_name = group_df['__BaseName'].iloc[0] if not group_df['__BaseName'].empty else ''
        if base_name:
            unique_base_names.add(base_name)
    
    logger.info(f"Unique base products: {len(unique_base_names):,}")
    
    # Partition groups evenly across batches while preserving order
    batch_dfs = []
    groups_per_batch = math.ceil(total_groups / num_batches) if valid_groups else 0
    
    for i in range(num_batches):
        start = i * groups_per_batch
        end = min((i + 1) * groups_per_batch, total_groups)
        batch_groups = valid_groups[start:end]
        
        if not batch_groups:
            batch_df = pd.DataFrame(columns=df.columns)
        else:
            batch_df = pd.concat([group_df for _, _, group_df in batch_groups]).sort_index()
        
        # Remove helper columns
        for col in ['__BaseName', '__ProductGroupID']:
            if col in batch_df.columns:
                batch_df = batch_df.drop(columns=[col])
        
        batch_dfs.append(batch_df)
        logger.info(f"Batch {i+1}: {len(batch_df):,} rows across {len(batch_groups)} product groups")
    
    # Verify counts match
    total_batch_rows = sum(len(batch) for batch in batch_dfs)
    if total_batch_rows != total_rows:
        logger.error(f"ERROR: Row count mismatch! Source rows: {total_rows}, Batches: {total_batch_rows}")
        raise ValueError(f"Row count mismatch: {total_rows} != {total_batch_rows}")
    
    logger.info(f"✓ Distributed {total_batch_rows:,} rows across {num_batches} batches")
    logger.info(f"✓ All {len(unique_base_names):,} base products included")
    return batch_dfs


def save_batch_csv(batch_df: pd.DataFrame, batch_num: int, temp_dir: Path) -> str:
    """Save a batch DataFrame to a temporary CSV file."""
    temp_dir.mkdir(parents=True, exist_ok=True)
    batch_file = temp_dir / f"batch_{batch_num}_source.csv"
    handler = CSVHandler()
    handler.write_csv(batch_df, str(batch_file))
    logger.info(f"Saved batch {batch_num} source CSV: {batch_file} ({len(batch_df):,} rows)")
    return str(batch_file)


def migrate_batch(
    batch_source_csv: str,
    batch_num: int,
    shopify_template: str,
    output_path: str,
    mapping_config: str,
    total_batches: int
) -> dict:
    """Migrate a single batch."""
    logger.info(f"\n{'='*80}")
    logger.info(f"BATCH {batch_num}/{total_batches} - Starting Migration")
    logger.info(f"{'='*80}")
    
    handler = CSVHandler()
    batch_df = handler.read_csv(batch_source_csv, low_memory=False)
    batch_size = len(batch_df)
    
    print(Fore.CYAN + f"\n{'='*80}")
    print(Fore.CYAN + f"BATCH {batch_num}/{total_batches} - {batch_size:,} Rows")
    print(Fore.CYAN + f"{'='*80}")
    print(f"Source: {batch_source_csv}")
    print(f"Output: {output_path}")
    print()
    
    try:
        # Initialize orchestrator with full batch (no sample_size limit)
        orchestrator = MigrationOrchestrator(
            source_csv_path=batch_source_csv,
            shopify_template_path=shopify_template,
            output_path=output_path,
            mapping_config_path=mapping_config,
            sample_size=None  # Process all rows in this batch
        )
        
        # Analyze source
        analysis = orchestrator.analyze_source()
        print(Fore.GREEN + f"✓ Batch {batch_num} source analyzed: {analysis['row_count']:,} rows")
        print()
        
        # Execute migration
        print(Fore.YELLOW + f"Migrating batch {batch_num}...")
        result = orchestrator.migrate()
        
        # Print results
        stats = result['statistics']
        print()
        print(Fore.GREEN + f"✓ Batch {batch_num} completed!")
        print(f"  Successfully migrated: {Fore.GREEN}{stats['successful_rows']:,}")
        print(f"  Failed rows: {Fore.RED}{stats['failed_rows']:,}")
        print(f"  Output file: {Fore.CYAN}{output_path}")
        
        return result
        
    except Exception as e:
        logger.exception(f"Batch {batch_num} migration failed")
        print(Fore.RED + f"\nERROR: Batch {batch_num} migration failed: {e}")
        raise


def verify_all_batches(output_files: list) -> dict:
    """Verify that all batches were created successfully and get summary stats."""
    print(Fore.CYAN + "\n" + "="*80)
    print(Fore.CYAN + "VERIFYING ALL BATCHES")
    print(Fore.CYAN + "="*80)
    
    handler = CSVHandler()
    total_rows = 0
    total_parents = 0
    total_variants = 0
    batch_stats = []
    
    for i, output_file in enumerate(output_files, 1):
        if not Path(output_file).exists():
            print(Fore.RED + f"❌ Batch {i} output file not found: {output_file}")
            continue
        
        df = handler.read_csv(output_file, low_memory=False)
        parent_rows = df[pd.notna(df['Title']) & (df['Title'] != '')]
        variant_rows = df[pd.isna(df['Title']) | (df['Title'] == '')]
        
        batch_rows = len(df)
        batch_parents = len(parent_rows)
        batch_variants = len(variant_rows)
        
        total_rows += batch_rows
        total_parents += batch_parents
        total_variants += batch_variants
        
        batch_stats.append({
            'batch': i,
            'file': output_file,
            'rows': batch_rows,
            'parents': batch_parents,
            'variants': batch_variants
        })
        
        print(Fore.GREEN + f"✓ Batch {i}: {batch_rows:,} rows ({batch_parents:,} parents, {batch_variants:,} variants)")
        print(f"  File: {output_file}")
        print(f"  Size: {Path(output_file).stat().st_size / 1024 / 1024:.2f} MB")
    
    print()
    print(Fore.CYAN + "="*80)
    print(Fore.CYAN + "BATCH MIGRATION SUMMARY")
    print(Fore.CYAN + "="*80)
    print(f"Total batches: {len(output_files)}")
    print(f"Total rows across all batches: {Fore.GREEN + f'{total_rows:,}'}")
    print(f"Total parent products: {Fore.GREEN + f'{total_parents:,}'}")
    print(f"Total variant rows: {Fore.GREEN + f'{total_variants:,}'}")
    print()
    
    return {
        'total_rows': total_rows,
        'total_parents': total_parents,
        'total_variants': total_variants,
        'batch_stats': batch_stats
    }


def main():
    """Main batch migration function."""
    parser = argparse.ArgumentParser(
        description='Migrate ALL products in 20 batches (including products with missing images/prices/descriptions)'
    )
    parser.add_argument('--source', type=str, help='Path to source products CSV file')
    parser.add_argument('--template', type=str, help='Path to Shopify template CSV file')
    parser.add_argument('--output-dir', type=str, help='Output directory for batch CSV files')
    parser.add_argument('--mapping', type=str, help='Path to field mapping configuration JSON')
    parser.add_argument('--config', type=str, default='config/config.yaml', help='Path to configuration YAML file')
    parser.add_argument('--num-batches', type=int, default=20, help='Number of batches to create (default: 20)')
    parser.add_argument('--batch-only', type=int, help='Run only a specific batch number (1-based)')
    parser.add_argument('--output-prefix', type=str, default='shopify_products_complete_batch', 
                       help='Prefix for output files (default: shopify_products_complete_batch)')
    
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
    
    output_dir = (
        args.output_dir or
        config.get('files', {}).get('output_dir') or
        'data/output'
    )
    
    mapping_config = (
        args.mapping or
        config.get('files', {}).get('mapping_config') or
        'config/field_mapping.json'
    )
    
    num_batches = args.num_batches
    output_prefix = args.output_prefix
    
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
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Create temporary directory for batch source files
    temp_dir = output_path / "batch_temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    print(Fore.CYAN + "=" * 80)
    print(Fore.CYAN + f"COMPLETE SHOPIFY PRODUCT MIGRATION - {num_batches} BATCHES")
    print(Fore.CYAN + "=" * 80)
    print(f"Source CSV: {source_csv}")
    print(f"Shopify Template: {shopify_template}")
    print(f"Output Directory: {output_path}")
    print(f"Number of Batches: {num_batches}")
    print(f"Output Prefix: {output_prefix}")
    print(Fore.GREEN + "✅ ALL products will be migrated (including those with missing images/prices/descriptions)")
    print(Fore.CYAN + "=" * 80)
    print()
    
    try:
        # Step 1: Split source CSV into batches
        print(Fore.YELLOW + "Step 1: Splitting source CSV into batches...")
        batches = split_source_csv_all_products(source_csv, num_batches)
        print(Fore.GREEN + f"✓ Source CSV split into {num_batches} batches")
        print()
        
        # Step 2: Save batch source files and migrate each batch
        output_files = []
        batch_source_files = []
        
        # Determine which batches to run
        if args.batch_only:
            if args.batch_only < 1 or args.batch_only > num_batches:
                print(Fore.RED + f"ERROR: Batch number must be between 1 and {num_batches}")
                sys.exit(1)
            batches_to_run = [args.batch_only - 1]  # Convert to 0-based index
            print(Fore.YELLOW + f"Running only batch {args.batch_only} of {num_batches}")
        else:
            batches_to_run = range(num_batches)
            print(Fore.YELLOW + f"Running all {num_batches} batches sequentially")
        
        for i, batch_df in enumerate(batches, 1):
            # Skip if not in batches_to_run
            if (i - 1) not in batches_to_run:
                # Still create output file path for verification
                output_file = output_path / f"{output_prefix}_{i}_of_{num_batches}.csv"
                output_files.append(str(output_file))
                continue
            
            # Save batch source CSV
            batch_source_file = save_batch_csv(batch_df, i, temp_dir)
            batch_source_files.append(batch_source_file)
            
            # Set output file path
            output_file = output_path / f"{output_prefix}_{i}_of_{num_batches}.csv"
            output_files.append(str(output_file))
            
            # Migrate this batch
            result = migrate_batch(
                batch_source_csv=batch_source_file,
                batch_num=i,
                shopify_template=shopify_template,
                output_path=str(output_file),
                mapping_config=mapping_config,
                total_batches=num_batches
            )
            
            print()
        
        # Step 3: Verify all batches
        print()
        verification = verify_all_batches(output_files)
        
        # Step 4: Cleanup temporary batch source files
        print(Fore.YELLOW + "\nCleaning up temporary files...")
        for batch_file in batch_source_files:
            try:
                Path(batch_file).unlink()
                logger.info(f"Removed temporary file: {batch_file}")
            except Exception as e:
                logger.warning(f"Could not remove temporary file {batch_file}: {e}")
        
        print(Fore.GREEN + "✓ Cleanup complete")
        
        # Final summary
        print()
        print(Fore.GREEN + "=" * 80)
        print(Fore.GREEN + "ALL BATCHES COMPLETED SUCCESSFULLY!")
        print(Fore.GREEN + "=" * 80)
        print()
        print(f"Total batches created: {Fore.GREEN + str(num_batches)}")
        total_rows_str = f"{verification['total_rows']:,}"
        total_parents_str = f"{verification['total_parents']:,}"
        total_variants_str = f"{verification['total_variants']:,}"
        print(f"Total rows migrated: {Fore.GREEN + total_rows_str}")
        print(f"Total parent products: {Fore.GREEN + total_parents_str}")
        print(f"Total variant rows: {Fore.GREEN + total_variants_str}")
        print()
        print(Fore.CYAN + "Output files:")
        for i, output_file in enumerate(output_files, 1):
            if Path(output_file).exists():
                file_size = Path(output_file).stat().st_size / 1024 / 1024
                print(f"  Batch {i}: {Fore.CYAN + output_file} ({file_size:.2f} MB)")
            else:
                print(f"  Batch {i}: {Fore.YELLOW + output_file} (not created)")
        print()
        print(Fore.GREEN + "✅ All batches are ready for Shopify import!")
        print()
        
    except Exception as e:
        logger.exception("Batch migration failed")
        print(Fore.RED + f"\nERROR: Batch migration failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

