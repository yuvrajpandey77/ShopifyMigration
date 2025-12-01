#!/usr/bin/env python3
"""
Comprehensive Source CSV Analysis for Migration
Analyzes the source CSV to identify all products, missing fields (images, prices, descriptions),
and prepares statistics for migration planning.
"""

import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from loguru import logger
from colorama import init, Fore
import json

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.csv_handler import CSVHandler

# Initialize colorama
init(autoreset=True)


def analyze_source_csv(csv_path: str) -> dict:
    """
    Analyze source CSV for migration readiness.
    
    Returns:
        Dictionary with comprehensive analysis
    """
    handler = CSVHandler()
    df = handler.read_csv(csv_path)
    
    total_rows = len(df)
    
    # Key fields to check
    key_fields = {
        'Name': 'Title',
        'SKU': 'SKU',
        'Regular price': 'Price',
        'Sale price': 'Compare-at price',
        'Images': 'Product image URL',
        'Description': 'Body (HTML)',
        'Stock': 'Inventory quantity',
        'Categories': 'Product category'
    }
    
    analysis = {
        'total_rows': total_rows,
        'total_products': 0,
        'unique_products': 0,
        'field_analysis': {},
        'missing_summary': {},
        'products_missing_fields': {
            'missing_image': [],
            'missing_price': [],
            'missing_description': [],
            'missing_sku': [],
            'missing_name': []
        },
        'migration_readiness': {
            'can_migrate_all': True,
            'issues': []
        }
    }
    
    # Analyze each key field
    for source_field, shopify_field in key_fields.items():
        if source_field not in df.columns:
            analysis['field_analysis'][source_field] = {
                'exists': False,
                'missing_count': total_rows,
                'missing_percentage': 100.0,
                'has_data_count': 0,
                'has_data_percentage': 0.0
            }
            analysis['missing_summary'][shopify_field] = {
                'missing': total_rows,
                'percentage': 100.0
            }
            continue
        
        # Count missing values
        missing = df[source_field].isnull().sum()
        missing_pct = (missing / total_rows * 100) if total_rows > 0 else 0
        has_data = total_rows - missing
        has_data_pct = (has_data / total_rows * 100) if total_rows > 0 else 0
        
        # Check for empty strings
        if df[source_field].dtype == 'object':
            empty_strings = (df[source_field].astype(str).str.strip() == '').sum()
            missing += empty_strings
            has_data = total_rows - missing
            has_data_pct = (has_data / total_rows * 100) if total_rows > 0 else 0
        
        analysis['field_analysis'][source_field] = {
            'exists': True,
            'missing_count': int(missing),
            'missing_percentage': round(missing_pct, 2),
            'has_data_count': int(has_data),
            'has_data_percentage': round(has_data_pct, 2)
        }
        
        analysis['missing_summary'][shopify_field] = {
            'missing': int(missing),
            'percentage': round(missing_pct, 2)
        }
    
    # Identify products missing critical fields
    for idx, row in df.iterrows():
        product_info = {
            'row_index': int(idx),
            'name': str(row.get('Name', 'N/A'))[:50],
            'sku': str(row.get('SKU', 'N/A'))[:30]
        }
        
        # Check for missing Name
        name = row.get('Name', '')
        if pd.isna(name) or (isinstance(name, str) and name.strip() == ''):
            analysis['products_missing_fields']['missing_name'].append(product_info)
        
        # Check for missing SKU
        sku = row.get('SKU', '')
        if pd.isna(sku) or (isinstance(sku, str) and sku.strip() == ''):
            analysis['products_missing_fields']['missing_sku'].append(product_info)
        
        # Check for missing Images
        images = row.get('Images', '')
        if pd.isna(images) or (isinstance(images, str) and images.strip() == ''):
            analysis['products_missing_fields']['missing_image'].append(product_info)
        
        # Check for missing Price (both Regular and Sale)
        regular_price = row.get('Regular price', '')
        sale_price = row.get('Sale price', '')
        has_price = False
        if not pd.isna(regular_price) and isinstance(regular_price, str) and regular_price.strip():
            try:
                price_val = float(str(regular_price).replace('$', '').replace(',', '').strip())
                if price_val > 0:
                    has_price = True
            except:
                pass
        if not has_price and not pd.isna(sale_price) and isinstance(sale_price, str) and sale_price.strip():
            try:
                price_val = float(str(sale_price).replace('$', '').replace(',', '').strip())
                if price_val > 0:
                    has_price = True
            except:
                pass
        if not has_price:
            analysis['products_missing_fields']['missing_price'].append(product_info)
        
        # Check for missing Description
        description = row.get('Description', '')
        if pd.isna(description) or (isinstance(description, str) and description.strip() == ''):
            analysis['products_missing_fields']['missing_description'].append(product_info)
    
    # Count unique BASE products (not variants) - use normalize_product_name to group variants
    from src.migration import normalize_product_name, determine_product_group_id
    
    if 'Name' in df.columns:
        # Create base name column
        df['__BaseName'] = df['Name'].apply(normalize_product_name)
        df['__ProductGroupID'] = df.apply(determine_product_group_id, axis=1)
        
        # Count unique product groups (this gives us unique base products)
        analysis['unique_products'] = df['__ProductGroupID'].nunique()
        analysis['unique_base_names'] = df['__BaseName'].nunique()
        
        # Get list of unique base product names
        unique_base_names_list = df['__BaseName'].unique().tolist()
        unique_base_names_list = [name for name in unique_base_names_list if name and str(name).strip() != '']
        analysis['unique_base_product_names'] = sorted(unique_base_names_list)
        
        # Clean up temporary columns
        df.drop(columns=['__BaseName', '__ProductGroupID'], inplace=True, errors='ignore')
    else:
        analysis['unique_products'] = 0
        analysis['unique_base_names'] = 0
        analysis['unique_base_product_names'] = []
    
    analysis['total_products'] = analysis['unique_products']
    
    # Migration readiness assessment
    critical_missing = len(analysis['products_missing_fields']['missing_name'])
    if critical_missing > 0:
        analysis['migration_readiness']['can_migrate_all'] = False
        analysis['migration_readiness']['issues'].append(
            f"{critical_missing} products missing Name (critical field)"
        )
    
    # Check if we can still migrate (even with missing fields)
    # We can migrate if we have at least Name or SKU
    can_migrate = True
    if critical_missing == total_rows:
        can_migrate = False
        analysis['migration_readiness']['issues'].append(
            "All products missing Name - cannot migrate"
        )
    
    analysis['migration_readiness']['can_migrate_all'] = can_migrate
    
    return analysis


def print_analysis_report(analysis: dict):
    """Print a formatted analysis report."""
    print(Fore.CYAN + "=" * 80)
    print(Fore.CYAN + "SOURCE CSV MIGRATION ANALYSIS")
    print(Fore.CYAN + "=" * 80)
    print()
    
    # Summary
    print(Fore.YELLOW + "SUMMARY:")
    print(f"  Total Rows: {analysis['total_rows']}")
    print(f"  Unique Base Products: {analysis['unique_products']}")
    print(f"  Unique Base Product Names: {analysis.get('unique_base_names', analysis['unique_products'])}")
    print()
    
    # Show sample of unique base product names
    if analysis.get('unique_base_product_names'):
        print(Fore.YELLOW + "SAMPLE UNIQUE BASE PRODUCT NAMES (first 20):")
        for i, name in enumerate(analysis['unique_base_product_names'][:20], 1):
            print(f"  {i:3d}. {name}")
        if len(analysis['unique_base_product_names']) > 20:
            print(f"  ... and {len(analysis['unique_base_product_names']) - 20} more unique base products")
        print()
    
    # Field Analysis
    print(Fore.YELLOW + "FIELD ANALYSIS:")
    print(f"{'Field':<30} {'Missing':<15} {'Has Data':<15} {'Status'}")
    print("-" * 80)
    
    for source_field, data in analysis['field_analysis'].items():
        if not data['exists']:
            status = Fore.RED + "FIELD NOT FOUND"
        elif data['missing_count'] == 0:
            status = Fore.GREEN + "✓ COMPLETE"
        elif data['missing_percentage'] < 10:
            status = Fore.YELLOW + "⚠ MOSTLY COMPLETE"
        elif data['missing_percentage'] < 50:
            status = Fore.YELLOW + "⚠ PARTIAL"
        else:
            status = Fore.RED + "✗ MOSTLY MISSING"
        
        print(f"{source_field:<30} {data['missing_count']:<8} ({data['missing_percentage']:>5.1f}%)  "
              f"{data['has_data_count']:<8} ({data['has_data_percentage']:>5.1f}%)  {status}")
    
    print()
    
    # Missing Fields Summary
    print(Fore.YELLOW + "MISSING FIELDS SUMMARY:")
    print(f"  Missing Images: {len(analysis['products_missing_fields']['missing_image'])} "
          f"({len(analysis['products_missing_fields']['missing_image']) / analysis['total_rows'] * 100:.1f}%)")
    print(f"  Missing Price: {len(analysis['products_missing_fields']['missing_price'])} "
          f"({len(analysis['products_missing_fields']['missing_price']) / analysis['total_rows'] * 100:.1f}%)")
    print(f"  Missing Description: {len(analysis['products_missing_fields']['missing_description'])} "
          f"({len(analysis['products_missing_fields']['missing_description']) / analysis['total_rows'] * 100:.1f}%)")
    print(f"  Missing SKU: {len(analysis['products_missing_fields']['missing_sku'])} "
          f"({len(analysis['products_missing_fields']['missing_sku']) / analysis['total_rows'] * 100:.1f}%)")
    print(f"  Missing Name: {len(analysis['products_missing_fields']['missing_name'])} "
          f"({len(analysis['products_missing_fields']['missing_name']) / analysis['total_rows'] * 100:.1f}%)")
    print()
    
    # Migration Readiness
    print(Fore.YELLOW + "MIGRATION READINESS:")
    if analysis['migration_readiness']['can_migrate_all']:
        print(Fore.GREEN + "  ✓ CAN MIGRATE ALL PRODUCTS")
        print(Fore.GREEN + "  (Products with missing images/prices/descriptions will be migrated with defaults)")
    else:
        print(Fore.RED + "  ✗ CANNOT MIGRATE ALL PRODUCTS")
        for issue in analysis['migration_readiness']['issues']:
            print(Fore.RED + f"    - {issue}")
    print()
    
    # Sample of products with missing fields
    if analysis['products_missing_fields']['missing_image']:
        print(Fore.YELLOW + "SAMPLE PRODUCTS MISSING IMAGES (first 5):")
        for product in analysis['products_missing_fields']['missing_image'][:5]:
            print(f"  Row {product['row_index']}: {product['name']} (SKU: {product['sku']})")
        if len(analysis['products_missing_fields']['missing_image']) > 5:
            print(f"  ... and {len(analysis['products_missing_fields']['missing_image']) - 5} more")
        print()
    
    if analysis['products_missing_fields']['missing_price']:
        print(Fore.YELLOW + "SAMPLE PRODUCTS MISSING PRICE (first 5):")
        for product in analysis['products_missing_fields']['missing_price'][:5]:
            print(f"  Row {product['row_index']}: {product['name']} (SKU: {product['sku']})")
        if len(analysis['products_missing_fields']['missing_price']) > 5:
            print(f"  ... and {len(analysis['products_missing_fields']['missing_price']) - 5} more")
        print()
    
    if analysis['products_missing_fields']['missing_description']:
        print(Fore.YELLOW + "SAMPLE PRODUCTS MISSING DESCRIPTION (first 5):")
        for product in analysis['products_missing_fields']['missing_description'][:5]:
            print(f"  Row {product['row_index']}: {product['name']} (SKU: {product['sku']})")
        if len(analysis['products_missing_fields']['missing_description']) > 5:
            print(f"  ... and {len(analysis['products_missing_fields']['missing_description']) - 5} more")
        print()


def main():
    """Main analysis function."""
    parser = argparse.ArgumentParser(
        description='Analyze source CSV for migration readiness'
    )
    parser.add_argument(
        'csv_file',
        type=str,
        help='Path to source CSV file'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Output JSON file for analysis results'
    )
    
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Setup logging
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO"
    )
    
    csv_path = Path(args.csv_file)
    
    if not csv_path.exists():
        print(Fore.RED + f"ERROR: CSV file not found: {csv_path}")
        sys.exit(1)
    
    try:
        print(Fore.CYAN + f"Analyzing: {csv_path}")
        print()
        
        analysis = analyze_source_csv(str(csv_path))
        print_analysis_report(analysis)
        
        # Save to JSON if requested
        if args.output:
            output_path = Path(args.output)
            # Convert to JSON-serializable format
            json_data = {
                'total_rows': analysis['total_rows'],
                'total_products': analysis['total_products'],
                'unique_products': analysis['unique_products'],
                'field_analysis': analysis['field_analysis'],
                'missing_summary': analysis['missing_summary'],
                'migration_readiness': analysis['migration_readiness'],
                'products_missing_fields': {
                    'missing_image': {
                        'count': len(analysis['products_missing_fields']['missing_image']),
                        'sample': analysis['products_missing_fields']['missing_image'][:10]
                    },
                    'missing_price': {
                        'count': len(analysis['products_missing_fields']['missing_price']),
                        'sample': analysis['products_missing_fields']['missing_price'][:10]
                    },
                    'missing_description': {
                        'count': len(analysis['products_missing_fields']['missing_description']),
                        'sample': analysis['products_missing_fields']['missing_description'][:10]
                    },
                    'missing_sku': {
                        'count': len(analysis['products_missing_fields']['missing_sku']),
                        'sample': analysis['products_missing_fields']['missing_sku'][:10]
                    },
                    'missing_name': {
                        'count': len(analysis['products_missing_fields']['missing_name']),
                        'sample': analysis['products_missing_fields']['missing_name'][:10]
                    }
                }
            }
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, default=str)
            print(Fore.GREEN + f"✓ Analysis saved to: {output_path}")
        
    except Exception as e:
        logger.exception("Analysis failed")
        print(Fore.RED + f"\nERROR: Analysis failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

