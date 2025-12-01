#!/usr/bin/env python3
"""
Comprehensive Analysis of All 20 Batches
Analyzes product counts, variants, and compares with source to verify completeness.
"""

import sys
import pandas as pd
from pathlib import Path
import yaml
from colorama import init, Fore
from typing import Dict, Set, List, Tuple

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.migration import normalize_product_name, determine_product_group_id

init(autoreset=True)


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(Fore.YELLOW + f"Config file not found: {config_path}")
        return {}


def analyze_source_csv(source_csv_path: str) -> Dict:
    """
    Analyze source CSV to get total product counts.
    
    Returns:
        Dictionary with source statistics
    """
    print(Fore.CYAN + "\n" + "="*80)
    print(Fore.CYAN + "ANALYZING SOURCE CSV")
    print(Fore.CYAN + "="*80)
    
    if not Path(source_csv_path).exists():
        print(Fore.RED + f"❌ Source file not found: {source_csv_path}")
        return {
            'total_rows': 0,
            'unique_names': 0,
            'unique_base_names': 0,
            'product_groups': 0,
            'base_names_set': set(),
            'all_names_set': set()
        }
    
    try:
        print(f"Reading source CSV: {source_csv_path}")
        df = pd.read_csv(source_csv_path, dtype=str, keep_default_na=False, low_memory=False)
        
        total_rows = len(df)
        print(f"Total rows in source: {Fore.GREEN + f'{total_rows:,}'}")
        
        # Get Name column
        name_column = 'Name' if 'Name' in df.columns else df.columns[0]
        print(f"Using column: {Fore.CYAN + name_column}")
        
        # Get all product names
        all_names = df[name_column].astype(str).str.strip()
        all_names = all_names[all_names != '']
        all_names = all_names[all_names.str.lower() != 'nan']
        
        # Get unique names
        unique_names = set(all_names.unique())
        
        # Get base names (normalized)
        base_names = {normalize_product_name(name) for name in unique_names if normalize_product_name(name)}
        
        # Count product groups
        df['__BaseName'] = df[name_column].apply(normalize_product_name)
        df['__ProductGroupID'] = df.apply(determine_product_group_id, axis=1)
        product_groups = len(df['__ProductGroupID'].unique())
        
        print(f"Unique product names: {Fore.GREEN + f'{len(unique_names):,}'}")
        print(f"Unique base products (variants grouped): {Fore.GREEN + f'{len(base_names):,}'}")
        print(f"Product groups: {Fore.GREEN + f'{product_groups:,}'}")
        
        return {
            'total_rows': total_rows,
            'unique_names': len(unique_names),
            'unique_base_names': len(base_names),
            'product_groups': product_groups,
            'base_names_set': base_names,
            'all_names_set': unique_names
        }
        
    except Exception as e:
        print(Fore.RED + f"❌ Error reading source CSV: {e}")
        import traceback
        traceback.print_exc()
        return {
            'total_rows': 0,
            'unique_names': 0,
            'unique_base_names': 0,
            'product_groups': 0,
            'base_names_set': set(),
            'all_names_set': set()
        }


def analyze_batch(batch_file: str, batch_num: int) -> Dict:
    """
    Analyze a single batch file.
    
    Returns:
        Dictionary with batch statistics
    """
    if not Path(batch_file).exists():
        return {
            'exists': False,
            'rows': 0,
            'parents': 0,
            'variants': 0,
            'image_rows': 0,
            'handles': set(),
            'titles': set(),
            'base_names': set(),
            'file_size_mb': 0
        }
    
    try:
        df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
        
        total_rows = len(df)
        
        # Separate parent rows (with Title) and variant rows (blank Title)
        parent_rows = df[df['Title'].astype(str).str.strip() != '']
        variant_rows = df[df['Title'].astype(str).str.strip() == '']
        
        # Count image rows (rows with Image Src but blank Title)
        image_rows = variant_rows[
            (variant_rows['Image Src'].astype(str).str.strip() != '') &
            (variant_rows['Image Src'].astype(str).str.strip().str.lower() != 'nan')
        ]
        
        # True variant rows (have Option1 Value)
        true_variant_rows = variant_rows[
            (variant_rows['Option1 Value'].astype(str).str.strip() != '') &
            (variant_rows['Option1 Value'].astype(str).str.strip().str.lower() != 'nan')
        ]
        
        # Get unique handles and titles
        handles = {h for h in parent_rows['Handle'].astype(str).str.strip().unique() 
                  if h and h.lower() != 'nan'}
        titles = {t for t in parent_rows['Title'].astype(str).str.strip().unique() 
                 if t and t.lower() != 'nan'}
        base_names = {normalize_product_name(t) for t in titles if normalize_product_name(t)}
        
        file_size_mb = Path(batch_file).stat().st_size / 1024 / 1024
        
        return {
            'exists': True,
            'rows': total_rows,
            'parents': len(parent_rows),
            'variants': len(true_variant_rows),
            'image_rows': len(image_rows) - len(true_variant_rows),
            'handles': handles,
            'titles': titles,
            'base_names': base_names,
            'file_size_mb': file_size_mb
        }
        
    except Exception as e:
        print(Fore.RED + f"❌ Error reading batch {batch_num}: {e}")
        return {
            'exists': False,
            'rows': 0,
            'parents': 0,
            'variants': 0,
            'image_rows': 0,
            'handles': set(),
            'titles': set(),
            'base_names': set(),
            'file_size_mb': 0
        }


def analyze_all_batches(num_batches: int = 20, output_dir: str = "data/output") -> Dict:
    """
    Analyze all batch files.
    
    Returns:
        Dictionary with aggregated statistics
    """
    print(Fore.CYAN + "\n" + "="*80)
    print(Fore.CYAN + f"ANALYZING ALL {num_batches} BATCHES")
    print(Fore.CYAN + "="*80)
    
    all_handles = set()
    all_titles = set()
    all_base_names = set()
    total_rows = 0
    total_parents = 0
    total_variants = 0
    total_image_rows = 0
    total_size_mb = 0
    
    batch_stats = []
    missing_batches = []
    
    for batch_num in range(1, num_batches + 1):
        batch_file = Path(output_dir) / f"shopify_products_batch_{batch_num}_of_{num_batches}.csv"
        stats = analyze_batch(str(batch_file), batch_num)
        
        if not stats['exists']:
            missing_batches.append(batch_num)
            print(Fore.RED + f"❌ Batch {batch_num}: File not found")
            continue
        
        all_handles.update(stats['handles'])
        all_titles.update(stats['titles'])
        all_base_names.update(stats['base_names'])
        total_rows += stats['rows']
        total_parents += stats['parents']
        total_variants += stats['variants']
        total_image_rows += stats['image_rows']
        total_size_mb += stats['file_size_mb']
        
        batch_stats.append({
            'batch': batch_num,
            'rows': stats['rows'],
            'parents': stats['parents'],
            'variants': stats['variants'],
            'image_rows': stats['image_rows'],
            'handles': len(stats['handles']),
            'titles': len(stats['titles']),
            'base_names': len(stats['base_names']),
            'size_mb': stats['file_size_mb']
        })
        
        print(Fore.GREEN + f"✓ Batch {batch_num:2d}: {stats['rows']:6,} rows | "
              f"{stats['parents']:5,} parents | {stats['variants']:5,} variants | "
              f"{len(stats['handles']):5,} products | {stats['file_size_mb']:6.2f} MB")
    
    print()
    if missing_batches:
        print(Fore.RED + f"⚠️  Missing batches: {missing_batches}")
    else:
        print(Fore.GREEN + f"✅ All {num_batches} batches found!")
    
    return {
        'total_rows': total_rows,
        'total_parents': total_parents,
        'total_variants': total_variants,
        'total_image_rows': total_image_rows,
        'total_handles': len(all_handles),
        'total_titles': len(all_titles),
        'total_base_names': len(all_base_names),
        'total_size_mb': total_size_mb,
        'batch_stats': batch_stats,
        'missing_batches': missing_batches,
        'all_handles': all_handles,
        'all_titles': all_titles,
        'all_base_names': all_base_names
    }


def compare_source_vs_migrated(source_stats: Dict, batch_stats: Dict) -> Dict:
    """
    Compare source statistics with migrated batch statistics.
    
    Returns:
        Dictionary with comparison results
    """
    print(Fore.CYAN + "\n" + "="*80)
    print(Fore.CYAN + "COMPARING SOURCE vs MIGRATED")
    print(Fore.CYAN + "="*80)
    
    source_base = source_stats['base_names_set']
    migrated_base = batch_stats['all_base_names']
    
    missing_base = source_base - migrated_base
    extra_base = migrated_base - source_base
    
    source_unique = source_stats['all_names_set']
    migrated_titles = batch_stats['all_titles']
    
    missing_titles = source_unique - migrated_titles
    extra_titles = migrated_titles - source_unique
    
    coverage_base = (len(migrated_base) / len(source_base) * 100) if source_base else 0
    coverage_titles = (len(migrated_titles) / len(source_unique) * 100) if source_unique else 0
    
    print(f"\n📊 BASE PRODUCTS (Variants Grouped):")
    print(f"  Source:        {Fore.CYAN + f'{len(source_base):,}'}")
    print(f"  Migrated:      {Fore.GREEN + f'{len(migrated_base):,}'}")
    print(f"  Missing:       {Fore.RED + f'{len(missing_base):,}'}")
    print(f"  Extra:         {Fore.YELLOW + f'{len(extra_base):,}'}")
    print(f"  Coverage:      {Fore.GREEN + f'{coverage_base:.2f}%' if coverage_base >= 99 else Fore.YELLOW + f'{coverage_base:.2f}%'}")
    
    print(f"\n📦 UNIQUE PRODUCT NAMES:")
    print(f"  Source:        {Fore.CYAN + f'{len(source_unique):,}'}")
    print(f"  Migrated:      {Fore.GREEN + f'{len(migrated_titles):,}'}")
    print(f"  Missing:       {Fore.RED + f'{len(missing_titles):,}'}")
    print(f"  Extra:         {Fore.YELLOW + f'{len(extra_titles):,}'}")
    print(f"  Coverage:      {Fore.GREEN + f'{coverage_titles:.2f}%' if coverage_titles >= 99 else Fore.YELLOW + f'{coverage_titles:.2f}%'}")
    
    if missing_base:
        print(f"\n⚠️  Missing Base Products ({len(missing_base)}):")
        for i, name in enumerate(sorted(missing_base)[:20], 1):
            print(f"  {i}. {name}")
        if len(missing_base) > 20:
            print(f"  ... and {len(missing_base) - 20} more")
    
    return {
        'missing_base': missing_base,
        'extra_base': extra_base,
        'missing_titles': missing_titles,
        'extra_titles': extra_titles,
        'coverage_base': coverage_base,
        'coverage_titles': coverage_titles
    }


def main():
    """Main analysis function."""
    print(Fore.CYAN + "="*80)
    print(Fore.CYAN + "COMPREHENSIVE ANALYSIS: ALL 20 BATCHES")
    print(Fore.CYAN + "="*80)
    
    # Load config
    config = load_config()
    source_csv = (
        config.get('files', {}).get('source_csv') or
        '/home/yuvraj/Documents/products.csv'
    )
    output_dir = config.get('files', {}).get('output_dir') or 'data/output'
    num_batches = 20
    
    # Step 1: Analyze source CSV
    source_stats = analyze_source_csv(source_csv)
    
    # Step 2: Analyze all batches
    batch_stats = analyze_all_batches(num_batches=num_batches, output_dir=output_dir)
    
    # Step 3: Compare source vs migrated
    comparison = compare_source_vs_migrated(source_stats, batch_stats)
    
    # Step 4: Final Summary
    print(Fore.CYAN + "\n" + "="*80)
    print(Fore.CYAN + "FINAL SUMMARY")
    print(Fore.CYAN + "="*80)
    
    print(f"\n📊 SOURCE STATISTICS:")
    total_rows_str = f"{source_stats['total_rows']:,}"
    unique_names_str = f"{source_stats['unique_names']:,}"
    unique_base_str = f"{source_stats['unique_base_names']:,}"
    product_groups_str = f"{source_stats['product_groups']:,}"
    print(f"  Total rows:              {Fore.CYAN + total_rows_str}")
    print(f"  Unique product names:    {Fore.CYAN + unique_names_str}")
    print(f"  Unique base products:    {Fore.CYAN + unique_base_str}")
    print(f"  Product groups:         {Fore.CYAN + product_groups_str}")
    
    print(f"\n📦 MIGRATED STATISTICS (All {num_batches} Batches):")
    batch_total_rows_str = f"{batch_stats['total_rows']:,}"
    batch_parents_str = f"{batch_stats['total_parents']:,}"
    batch_variants_str = f"{batch_stats['total_variants']:,}"
    batch_images_str = f"{batch_stats['total_image_rows']:,}"
    batch_handles_str = f"{batch_stats['total_handles']:,}"
    batch_titles_str = f"{batch_stats['total_titles']:,}"
    batch_base_str = f"{batch_stats['total_base_names']:,}"
    batch_size_str = f"{batch_stats['total_size_mb']:.2f} MB"
    print(f"  Total rows:              {Fore.GREEN + batch_total_rows_str}")
    print(f"  Parent products:        {Fore.GREEN + batch_parents_str}")
    print(f"  Variant rows:           {Fore.GREEN + batch_variants_str}")
    print(f"  Image rows:             {Fore.GREEN + batch_images_str}")
    print(f"  Unique products (Handle): {Fore.GREEN + batch_handles_str}")
    print(f"  Unique products (Title):  {Fore.GREEN + batch_titles_str}")
    print(f"  Unique base products:     {Fore.GREEN + batch_base_str}")
    print(f"  Total file size:          {Fore.GREEN + batch_size_str}")
    
    print(f"\n✅ COMPLETENESS CHECK:")
    coverage_base_str = f"{comparison['coverage_base']:.2f}%"
    coverage_titles_str = f"{comparison['coverage_titles']:.2f}%"
    if comparison['coverage_base'] >= 99.5:
        print(f"  Base Products Coverage:  {Fore.GREEN + coverage_base_str} ✅ EXCELLENT")
    elif comparison['coverage_base'] >= 95:
        print(f"  Base Products Coverage:  {Fore.YELLOW + coverage_base_str} ⚠️  GOOD")
    else:
        print(f"  Base Products Coverage:  {Fore.RED + coverage_base_str} ❌ NEEDS ATTENTION")
    
    if comparison['coverage_titles'] >= 99.5:
        print(f"  Product Names Coverage:  {Fore.GREEN + coverage_titles_str} ✅ EXCELLENT")
    elif comparison['coverage_titles'] >= 95:
        print(f"  Product Names Coverage:  {Fore.YELLOW + coverage_titles_str} ⚠️  GOOD")
    else:
        print(f"  Product Names Coverage:  {Fore.RED + coverage_titles_str} ❌ NEEDS ATTENTION")
    
    print(f"\n📈 MISSING PRODUCTS:")
    missing_base_str = f"{len(comparison['missing_base']):,}"
    missing_titles_str = f"{len(comparison['missing_titles']):,}"
    print(f"  Missing base products:  {Fore.RED + missing_base_str}")
    print(f"  Missing product names:  {Fore.RED + missing_titles_str}")
    
    if len(comparison['missing_base']) == 0 and len(comparison['missing_titles']) == 0:
        print(f"\n{Fore.GREEN + '='*80}")
        print(Fore.GREEN + "🎉 SUCCESS! ALL PRODUCTS HAVE BEEN MIGRATED! 🎉")
        print(Fore.GREEN + "="*80)
    elif len(comparison['missing_base']) < 10:
        print(f"\n{Fore.YELLOW + '='*80}")
        print(Fore.YELLOW + f"⚠️  Almost complete! Only {len(comparison['missing_base'])} base products missing.")
        print(Fore.YELLOW + "="*80)
    else:
        print(f"\n{Fore.RED + '='*80}")
        print(Fore.RED + f"❌ INCOMPLETE: {len(comparison['missing_base'])} base products are missing!")
        print(Fore.RED + "="*80)
    
    print()


if __name__ == '__main__':
    main()

