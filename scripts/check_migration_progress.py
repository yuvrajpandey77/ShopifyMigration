#!/usr/bin/env python3
"""
Check progress of the complete 20-batch migration.
"""

import sys
from pathlib import Path
from colorama import init, Fore
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.csv_handler import CSVHandler

init(autoreset=True)


def check_progress(output_dir: str = "data/output", prefix: str = "shopify_products_complete_batch", num_batches: int = 20):
    """Check migration progress."""
    print(Fore.CYAN + "="*80)
    print(Fore.CYAN + "MIGRATION PROGRESS CHECK")
    print(Fore.CYAN + "="*80)
    print()
    
    output_path = Path(output_dir)
    handler = CSVHandler()
    
    completed = []
    total_rows = 0
    total_parents = 0
    total_variants = 0
    
    for i in range(1, num_batches + 1):
        batch_file = output_path / f"{prefix}_{i}_of_{num_batches}.csv"
        if batch_file.exists():
            try:
                df = handler.read_csv(str(batch_file), low_memory=False)
                parent_rows = df[pd.notna(df['Title']) & (df['Title'] != '')]
                variant_rows = df[pd.isna(df['Title']) | (df['Title'] == '')]
                
                batch_rows = len(df)
                batch_parents = len(parent_rows)
                batch_variants = len(variant_rows)
                
                total_rows += batch_rows
                total_parents += batch_parents
                total_variants += batch_variants
                
                file_size = batch_file.stat().st_size / 1024 / 1024
                completed.append(i)
                
                print(Fore.GREEN + f"✓ Batch {i:2d}: {batch_rows:6,} rows | "
                      f"{batch_parents:5,} parents | {batch_variants:5,} variants | "
                      f"{file_size:6.2f} MB")
            except Exception as e:
                print(Fore.YELLOW + f"⚠ Batch {i:2d}: File exists but error reading: {e}")
        else:
            print(Fore.RED + f"✗ Batch {i:2d}: Not yet created")
    
    print()
    print(Fore.CYAN + "="*80)
    print(f"Progress: {len(completed)}/{num_batches} batches completed ({len(completed)/num_batches*100:.1f}%)")
    print(f"Total rows migrated so far: {Fore.GREEN + f'{total_rows:,}'}")
    print(f"Total parent products: {Fore.GREEN + f'{total_parents:,}'}")
    print(f"Total variant rows: {Fore.GREEN + f'{total_variants:,}'}")
    print(Fore.CYAN + "="*80)
    
    if len(completed) == num_batches:
        print(Fore.GREEN + "\n🎉 ALL BATCHES COMPLETED! 🎉")
    elif len(completed) > 0:
        print(Fore.YELLOW + f"\n⏳ Migration in progress... {num_batches - len(completed)} batches remaining")
    else:
        print(Fore.RED + "\n❌ No batches completed yet")


if __name__ == '__main__':
    check_progress()

