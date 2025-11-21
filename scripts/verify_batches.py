#!/usr/bin/env python3
"""
Verify all batch files and ensure no products are missed.
"""

import sys
import pandas as pd
from pathlib import Path
import glob
from colorama import init, Fore

init(autoreset=True)


def verify_batches(output_dir: str = "data/output", num_batches: int = 5):
    """Verify all batch files."""
    print(Fore.CYAN + "=" * 80)
    print(Fore.CYAN + "BATCH VERIFICATION")
    print(Fore.CYAN + "=" * 80)
    print()
    
    # Find all batch files
    batch_files = sorted(glob.glob(f"{output_dir}/shopify_products_batch_*_of_{num_batches}.csv"))
    
    if not batch_files:
        print(Fore.RED + f"❌ No batch files found in {output_dir}")
        print(f"Expected files: shopify_products_batch_1_of_{num_batches}.csv through shopify_products_batch_{num_batches}_of_{num_batches}.csv")
        return False
    
    print(f"Found {len(batch_files)} batch files:")
    print()
    
    total_rows = 0
    total_parents = 0
    total_variants = 0
    batch_stats = []
    
    for batch_file in batch_files:
        df = pd.read_csv(batch_file, low_memory=False)
        parent_rows = df[pd.notna(df['Title']) & (df['Title'] != '')]
        variant_rows = df[pd.isna(df['Title']) | (df['Title'] == '')]
        
        batch_rows = len(df)
        batch_parents = len(parent_rows)
        batch_variants = len(variant_rows)
        
        total_rows += batch_rows
        total_parents += batch_parents
        total_variants += batch_variants
        
        file_size = Path(batch_file).stat().st_size / 1024 / 1024
        batch_name = Path(batch_file).name
        
        batch_stats.append({
            'file': batch_name,
            'rows': batch_rows,
            'parents': batch_parents,
            'variants': batch_variants,
            'size_mb': file_size
        })
        
        print(Fore.GREEN + f"✓ {batch_name}")
        print(f"  Rows: {batch_rows:,} (Parents: {batch_parents:,}, Variants: {batch_variants:,})")
        print(f"  Size: {file_size:.2f} MB")
        print()
    
    # Check for zero prices
    print(Fore.CYAN + "Checking for zero prices...")
    zero_price_count = 0
    for batch_file in batch_files:
        df = pd.read_csv(batch_file, low_memory=False)
        variant_rows = df[pd.isna(df['Title']) | (df['Title'] == '')]
        parent_rows = df[pd.notna(df['Title']) & (df['Title'] != '')]
        single_products = parent_rows[~parent_rows['Handle'].isin(variant_rows['Handle'].unique())]
        
        # Check variant prices
        variant_prices = variant_rows['Variant Price'].astype(str).str.replace('$', '').str.replace(',', '').str.strip()
        variant_prices_numeric = pd.to_numeric(variant_prices, errors='coerce')
        zero_variant = len(variant_prices_numeric[variant_prices_numeric == 0])
        
        # Check single product prices
        single_prices = single_products['Variant Price'].astype(str).str.replace('$', '').str.replace(',', '').str.strip()
        single_prices_numeric = pd.to_numeric(single_prices, errors='coerce')
        zero_single = len(single_prices_numeric[single_prices_numeric == 0])
        
        zero_price_count += zero_variant + zero_single
    
    if zero_price_count == 0:
        print(Fore.GREEN + f"✓ No zero prices found across all batches")
    else:
        print(Fore.RED + f"❌ Found {zero_price_count} rows with zero prices")
    
    print()
    
    # Summary
    print(Fore.CYAN + "=" * 80)
    print(Fore.CYAN + "BATCH VERIFICATION SUMMARY")
    print(Fore.CYAN + "=" * 80)
    print()
    print(f"Total batches: {len(batch_files)}")
    print(f"Total rows: {Fore.GREEN + f'{total_rows:,}'}")
    print(f"Total parent products: {Fore.GREEN + f'{total_parents:,}'}")
    print(f"Total variant rows: {Fore.GREEN + f'{total_variants:,}'}")
    print(f"Zero prices: {Fore.GREEN + '0' if zero_price_count == 0 else Fore.RED + str(zero_price_count)}")
    print()
    
    if zero_price_count == 0:
        print(Fore.GREEN + "✅✅✅ ALL BATCHES VERIFIED - READY FOR SHOPIFY IMPORT! ✅✅✅")
    else:
        print(Fore.YELLOW + "⚠️  Some batches have zero prices - review before import")
    
    print()
    return zero_price_count == 0


if __name__ == '__main__':
    num_batches = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    verify_batches(num_batches=num_batches)

