#!/usr/bin/env python3
"""
Analyze Migration Output
Analyzes the migrated CSV to check field coverage and Shopify compliance.
"""

import sys
import pandas as pd
from pathlib import Path
from colorama import init, Fore
import json

init(autoreset=True)


def analyze_migration_output(csv_path: str, template_path: str):
    """Analyze migration output for completeness and compliance."""
    
    print(Fore.CYAN + "=" * 70)
    print(Fore.CYAN + "MIGRATION OUTPUT ANALYSIS")
    print(Fore.CYAN + "=" * 70)
    print()
    
    # Load migrated CSV
    df = pd.read_csv(csv_path)
    template_df = pd.read_csv(template_path)
    
    print(Fore.GREEN + f"✓ Loaded migrated CSV: {len(df)} products")
    print(Fore.GREEN + f"✓ Shopify template: {len(template_df.columns)} fields")
    print()
    
    # Check if all template columns are present
    template_cols = set(template_df.columns)
    output_cols = set(df.columns)
    missing_cols = template_cols - output_cols
    extra_cols = output_cols - template_cols
    
    if missing_cols:
        print(Fore.RED + f"⚠ Missing columns in output: {len(missing_cols)}")
        for col in sorted(missing_cols):
            print(f"  - {col}")
    else:
        print(Fore.GREEN + "✓ All template columns present")
    
    if extra_cols:
        print(Fore.YELLOW + f"⚠ Extra columns in output: {len(extra_cols)}")
        for col in sorted(extra_cols):
            print(f"  - {col}")
    
    print()
    
    # Analyze field coverage
    print(Fore.YELLOW + "FIELD COVERAGE ANALYSIS")
    print("-" * 70)
    
    # Required fields (Shopify typically requires these)
    critical_fields = [
        'Title', 'SKU', 'Price', 'Status', 'Inventory tracker', 'Inventory quantity'
    ]
    
    # Important fields
    important_fields = [
        'Description', 'Vendor', 'Type', 'Tags', 'Published on online store',
        'Product image URL', 'URL handle', 'SEO title', 'SEO description'
    ]
    
    analysis = {
        'total_products': len(df),
        'critical_fields': {},
        'important_fields': {},
        'all_fields': {},
        'field_statistics': {}
    }
    
    # Analyze critical fields
    print(Fore.CYAN + "\nCritical Fields:")
    for field in critical_fields:
        if field in df.columns:
            non_empty = df[field].notna().sum()
            empty = df[field].isna().sum()
            pct = (non_empty / len(df)) * 100
            
            analysis['critical_fields'][field] = {
                'populated': non_empty,
                'empty': empty,
                'percentage': pct
            }
            
            status = Fore.GREEN + "✓" if pct >= 90 else Fore.YELLOW + "⚠" if pct >= 50 else Fore.RED + "✗"
            print(f"  {status} {field:30s} {non_empty:4d}/{len(df)} ({pct:5.1f}%) populated")
        else:
            print(f"  {Fore.RED}✗ {field:30s} MISSING COLUMN")
            analysis['critical_fields'][field] = {'populated': 0, 'empty': len(df), 'percentage': 0}
    
    # Analyze important fields
    print(Fore.CYAN + "\nImportant Fields:")
    for field in important_fields:
        if field in df.columns:
            non_empty = df[field].notna().sum()
            empty = df[field].isna().sum()
            pct = (non_empty / len(df)) * 100
            
            analysis['important_fields'][field] = {
                'populated': non_empty,
                'empty': empty,
                'percentage': pct
            }
            
            status = Fore.GREEN + "✓" if pct >= 50 else Fore.YELLOW + "⚠" if pct >= 10 else Fore.RED + "✗"
            print(f"  {status} {field:30s} {non_empty:4d}/{len(df)} ({pct:5.1f}%) populated")
        else:
            print(f"  {Fore.RED}✗ {field:30s} MISSING COLUMN")
            analysis['important_fields'][field] = {'populated': 0, 'empty': len(df), 'percentage': 0}
    
    # Analyze all fields
    print(Fore.CYAN + "\nAll Fields Coverage:")
    print("-" * 70)
    
    field_stats = []
    for col in sorted(df.columns):
        non_empty = df[col].notna().sum()
        empty = df[col].isna().sum()
        pct = (non_empty / len(df)) * 100
        
        field_stats.append({
            'field': col,
            'populated': non_empty,
            'empty': empty,
            'percentage': pct
        })
        
        analysis['all_fields'][col] = {
            'populated': non_empty,
            'empty': empty,
            'percentage': pct
        }
    
    # Sort by percentage
    field_stats.sort(key=lambda x: x['percentage'], reverse=True)
    
    # Show top and bottom fields
    print(Fore.GREEN + "\nTop 10 Most Populated Fields:")
    for stat in field_stats[:10]:
        print(f"  {stat['field']:40s} {stat['populated']:4d}/{len(df)} ({stat['percentage']:5.1f}%)")
    
    print(Fore.RED + "\nTop 10 Least Populated Fields:")
    for stat in field_stats[-10:]:
        if stat['percentage'] < 100:
            print(f"  {stat['field']:40s} {stat['populated']:4d}/{len(df)} ({stat['percentage']:5.1f}%)")
    
    # Data quality checks
    print()
    print(Fore.YELLOW + "DATA QUALITY CHECKS")
    print("-" * 70)
    
    issues = []
    
    # Check for duplicate SKUs
    if 'SKU' in df.columns:
        duplicates = df[df.duplicated(subset=['SKU'], keep=False)]
        if len(duplicates) > 0:
            issues.append(f"Duplicate SKUs: {len(duplicates)} products")
            print(Fore.RED + f"✗ Duplicate SKUs found: {len(duplicates)} products")
        else:
            print(Fore.GREEN + "✓ No duplicate SKUs")
    
    # Check for empty titles
    if 'Title' in df.columns:
        empty_titles = df[df['Title'].isna() | (df['Title'].astype(str).str.strip() == '')]
        if len(empty_titles) > 0:
            issues.append(f"Empty titles: {len(empty_titles)} products")
            print(Fore.RED + f"✗ Empty titles: {len(empty_titles)} products")
        else:
            print(Fore.GREEN + "✓ All products have titles")
    
    # Check for empty prices
    if 'Price' in df.columns:
        empty_prices = df[df['Price'].isna()]
        if len(empty_prices) > 0:
            issues.append(f"Empty prices: {len(empty_prices)} products")
            print(Fore.RED + f"✗ Empty prices: {len(empty_prices)} products")
        else:
            print(Fore.GREEN + "✓ All products have prices")
    
    # Check for invalid prices
    if 'Price' in df.columns:
        try:
            prices = pd.to_numeric(df['Price'], errors='coerce')
            invalid_prices = df[prices.isna() | (prices < 0)]
            if len(invalid_prices) > 0:
                issues.append(f"Invalid prices: {len(invalid_prices)} products")
                print(Fore.RED + f"✗ Invalid prices: {len(invalid_prices)} products")
            else:
                print(Fore.GREEN + "✓ All prices are valid")
        except:
            pass
    
    # Check URL handles
    if 'URL handle' in df.columns:
        handles = df['URL handle'].astype(str)
        invalid_handles = df[~handles.str.match(r'^[a-z0-9-]+$', na=False)]
        if len(invalid_handles) > 0:
            issues.append(f"Invalid URL handles: {len(invalid_handles)} products")
            print(Fore.YELLOW + f"⚠ Invalid URL handles: {len(invalid_handles)} products")
        else:
            print(Fore.GREEN + "✓ All URL handles are valid")
    
    # Summary
    print()
    print(Fore.CYAN + "=" * 70)
    print(Fore.CYAN + "SUMMARY")
    print(Fore.CYAN + "=" * 70)
    
    critical_avg = sum(f['percentage'] for f in analysis['critical_fields'].values()) / len(analysis['critical_fields']) if analysis['critical_fields'] else 0
    important_avg = sum(f['percentage'] for f in analysis['important_fields'].values()) / len(analysis['important_fields']) if analysis['important_fields'] else 0
    
    print(f"Total Products: {len(df)}")
    print(f"Critical Fields Coverage: {Fore.GREEN if critical_avg >= 90 else Fore.YELLOW if critical_avg >= 50 else Fore.RED}{critical_avg:.1f}%")
    print(f"Important Fields Coverage: {Fore.GREEN if important_avg >= 50 else Fore.YELLOW if important_avg >= 10 else Fore.RED}{important_avg:.1f}%")
    print(f"Total Fields: {len(df.columns)}")
    print(f"Data Quality Issues: {Fore.RED if issues else Fore.GREEN}{len(issues)}")
    
    if issues:
        print()
        print(Fore.YELLOW + "Issues to Address:")
        for issue in issues:
            print(f"  - {issue}")
    
    # Save analysis to JSON
    output_path = Path(csv_path).parent / "migration_analysis.json"
    with open(output_path, 'w') as f:
        json.dump(analysis, f, indent=2, default=str)
    
    print()
    print(Fore.GREEN + f"✓ Analysis saved to: {output_path}")
    
    return analysis


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python analyze_migration_output.py <migrated_csv> [template_csv]")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    template_path = sys.argv[2] if len(sys.argv) > 2 else '/home/yuvraj/Documents/productstemplate.csv'
    
    analyze_migration_output(csv_path, template_path)

