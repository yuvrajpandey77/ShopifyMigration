#!/usr/bin/env python3
"""
Migrate final batches 20-28 for the truly missing 503 products.
"""

import sys
from pathlib import Path
from colorama import Fore, Style, init

init(autoreset=True)

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.migrate_batches import migrate_batch, load_config
import pandas as pd

def main():
    """Migrate batches 20-28."""
    
    config = load_config()
    shopify_template = config.get('files', {}).get('shopify_template', '/home/yuvraj/Documents/product_template_csv_unit_price.csv')
    output_dir = Path(config.get('files', {}).get('output_dir', './data/output'))
    mapping_config = 'config/field_mapping.json'
    temp_dir = Path('data/temp_batches')
    
    batches_to_migrate = list(range(20, 29))
    total_batches = len(batches_to_migrate)
    
    print(Fore.CYAN + "="*80)
    print(Fore.CYAN + f"FINAL MIGRATION - BATCHES 20-28 ({total_batches} batches)")
    print(Fore.CYAN + "="*80)
    print()
    
    results = []
    
    for batch_num in batches_to_migrate:
        batch_source = temp_dir / f"batch_{batch_num}_source.csv"
        
        if not batch_source.exists():
            print(Fore.YELLOW + f"⚠️  Batch {batch_num} source file not found: {batch_source}")
            continue
        
        output_path = output_dir / f"shopify_products_batch_{batch_num}_of_28.csv"
        
        try:
            print(Fore.GREEN + f"\n{'='*80}")
            print(Fore.GREEN + f"Migrating Batch {batch_num}/{total_batches}")
            print(Fore.GREEN + f"{'='*80}")
            
            result = migrate_batch(
                batch_source_csv=str(batch_source),
                batch_num=batch_num,
                shopify_template=shopify_template,
                output_path=str(output_path),
                mapping_config=mapping_config,
                total_batches=total_batches
            )
            
            results.append({
                'batch': batch_num,
                'success': result.get('success', False),
                'output_file': str(output_path),
                'stats': result.get('stats', {})
            })
            
            if result.get('success'):
                print(Fore.GREEN + f"\n✅ Batch {batch_num} completed!")
                
                # Verify and fix
                df = pd.read_csv(output_path, dtype=str, keep_default_na=False, low_memory=False)
                parent_rows = df[df['Title'].astype(str).str.strip() != '']
                
                parent_handles = set(parent_rows['Handle'].astype(str).str.strip().unique())
                variant_handles = set(df[df['Title'].astype(str).str.strip() == '']['Handle'].astype(str).str.strip().unique())
                orphaned = variant_handles - parent_handles
                
                zero_price_count = 0
                for idx, row in parent_rows.iterrows():
                    price_fields = ['Price', 'Variant Price']
                    has_price = False
                    for field in price_fields:
                        if field in row:
                            price_str = str(row[field]).strip()
                            if price_str and price_str.lower() != 'nan':
                                try:
                                    price = float(price_str.replace('$', '').replace(',', '').strip())
                                    if price > 0:
                                        has_price = True
                                        break
                                except:
                                    pass
                    if not has_price:
                        zero_price_count += 1
                
                if orphaned:
                    print(Fore.YELLOW + f"  ⚠️  Found {len(orphaned)} orphaned handles - fixing...")
                    from scripts.fix_all_batches_6_10 import aggressive_fix_batch
                    aggressive_fix_batch(str(output_path))
                    print(Fore.GREEN + f"  ✅ Fixed!")
                
                if zero_price_count > 0:
                    print(Fore.YELLOW + f"  ⚠️  Found {zero_price_count} parent rows with zero prices - fixing...")
                    from scripts.fix_parent_prices_6_10 import fix_parent_prices
                    fix_parent_prices(str(output_path), str(batch_source))
                    print(Fore.GREEN + f"  ✅ Fixed!")
                
                if not orphaned and zero_price_count == 0:
                    print(Fore.GREEN + f"  ✅✅✅ Batch {batch_num} is perfect!")
        
        except Exception as e:
            print(Fore.RED + f"\n❌ Error migrating batch {batch_num}: {e}")
            results.append({
                'batch': batch_num,
                'success': False,
                'error': str(e)
            })
    
    # Summary
    print("\n" + "="*80)
    print(Fore.CYAN + "FINAL MIGRATION SUMMARY")
    print("="*80)
    
    successful = [r for r in results if r.get('success')]
    failed = [r for r in results if not r.get('success')]
    
    print(f"\n✅ Successful batches: {len(successful)}/{total_batches}")
    total_products = 0
    for r in successful:
        if Path(r['output_file']).exists():
            df = pd.read_csv(r['output_file'], dtype=str, keep_default_na=False, low_memory=False)
            parent_rows = df[df['Title'].astype(str).str.strip() != '']
            products = len(parent_rows['Handle'].unique())
            total_products += products
            print(f"   Batch {r['batch']}: {products} products, {len(df):,} total rows")
    
    print(f"\n📊 TOTAL NEW PRODUCTS MIGRATED: {total_products:,}")
    
    if failed:
        print(f"\n❌ Failed batches: {len(failed)}/{total_batches}")
        for r in failed:
            print(f"   Batch {r['batch']}: {r.get('error', 'Unknown error')}")
    
    print("\n" + "="*80)
    print(Fore.GREEN + "FINAL MIGRATION COMPLETE!")
    print("="*80)

if __name__ == "__main__":
    main()

