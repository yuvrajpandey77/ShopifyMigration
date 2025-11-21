#!/usr/bin/env python3
"""
Check migration status and progress.
"""

import sys
from pathlib import Path
import pandas as pd
import glob
from colorama import init, Fore

init(autoreset=True)

def check_status():
    """Check migration status."""
    print(Fore.CYAN + "=" * 80)
    print(Fore.CYAN + "MIGRATION STATUS CHECK")
    print(Fore.CYAN + "=" * 80)
    print()
    
    # Check source CSV
    source_csv = "/home/yuvraj/Documents/products.csv"
    if Path(source_csv).exists():
        try:
            df = pd.read_csv(source_csv, low_memory=False)
            total_products = len(df)
            print(f"📊 Source CSV: {Fore.GREEN + f'{total_products:,}'} products")
        except Exception as e:
            print(f"❌ Error reading source CSV: {e}")
            total_products = 0
    else:
        print(f"❌ Source CSV not found: {source_csv}")
        total_products = 0
    
    print()
    
    # Check batch files
    output_dir = Path("data/output")
    batch_files = sorted(glob.glob(str(output_dir / "shopify_products_batch_*_of_*.csv")))
    
    print(f"📦 Batch Files: {Fore.CYAN + str(len(batch_files))} found")
    print()
    
    if batch_files:
        total_migrated_rows = 0
        total_parents = 0
        total_variants = 0
        
        for batch_file in batch_files:
            try:
                df = pd.read_csv(batch_file, low_memory=False)
                rows = len(df)
                parents = len(df[pd.notna(df['Title']) & (df['Title'] != '')])
                variants = rows - parents
                
                total_migrated_rows += rows
                total_parents += parents
                total_variants += variants
                
                file_size = Path(batch_file).stat().st_size / 1024 / 1024
                batch_name = Path(batch_file).name
                
                print(f"  ✓ {Fore.GREEN + batch_name}")
                print(f"    Rows: {rows:,} (Parents: {parents:,}, Variants: {variants:,})")
                print(f"    Size: {file_size:.2f} MB")
                print()
            except Exception as e:
                print(f"  ❌ {Path(batch_file).name}: Error - {e}")
                print()
        
        print(Fore.CYAN + "-" * 80)
        print(f"Total migrated rows: {Fore.GREEN + f'{total_migrated_rows:,}'}")
        print(f"Total parent products: {Fore.GREEN + f'{total_parents:,}'}")
        print(f"Total variant rows: {Fore.GREEN + f'{total_variants:,}'}")
        
        if total_products > 0:
            # Estimate progress (rows might be more than products due to variants)
            # Each product can have multiple variant rows
            progress_pct = min(100, (total_migrated_rows / (total_products * 1.5)) * 100)
            print()
            print(f"📈 Estimated Progress: {Fore.YELLOW + f'{progress_pct:.1f}%'}")
    else:
        print("  ⚠️  No batch files found")
        print("  Expected: shopify_products_batch_1_of_5.csv through shopify_products_batch_5_of_5.csv")
        print()
        print(f"📈 Progress: {Fore.RED + '0%'} - No batches completed yet")
    
    print()
    print(Fore.CYAN + "=" * 80)
    
    # Check for running processes
    import subprocess
    try:
        result = subprocess.run(['pgrep', '-af', 'migrate'], capture_output=True, text=True)
        if result.stdout.strip():
            print(f"🔄 Running processes found:")
            for line in result.stdout.strip().split('\n'):
                print(f"  {line}")
        else:
            print("⏸️  No migration processes currently running")
    except:
        print("⏸️  Could not check for running processes")
    
    print()

if __name__ == '__main__':
    check_status()

