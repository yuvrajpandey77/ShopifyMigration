#!/usr/bin/env python3
"""
Migrate batches 6-10 from pre-created batch source files.
"""

import sys
from pathlib import Path
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.migrate_batches import migrate_batch, load_config
import yaml

def main():
    """Migrate batches 6-10."""
    
    # Load config
    config = load_config()
    shopify_template = config.get('files', {}).get('shopify_template', '/home/yuvraj/Documents/product_template_csv_unit_price.csv')
    output_dir = Path(config.get('files', {}).get('output_dir', './data/output'))
    mapping_config = 'config/field_mapping.json'
    temp_dir = Path('data/temp_batches')
    
    # Batch files to migrate
    batches_to_migrate = [6, 7, 8, 9, 10]
    total_batches = len(batches_to_migrate)
    
    print(Fore.CYAN + "="*80)
    print(Fore.CYAN + f"MIGRATING BATCHES 6-10 ({total_batches} batches)")
    print(Fore.CYAN + "="*80)
    print()
    
    results = []
    
    for batch_num in batches_to_migrate:
        batch_source = temp_dir / f"batch_{batch_num}_source.csv"
        
        if not batch_source.exists():
            print(Fore.YELLOW + f"⚠️  Batch {batch_num} source file not found: {batch_source}")
            print(Fore.YELLOW + "   Skipping...")
            continue
        
        output_path = output_dir / f"shopify_products_batch_{batch_num}_of_10.csv"
        
        try:
            print(Fore.GREEN + f"\n{'='*80}")
            print(Fore.GREEN + f"Starting Batch {batch_num}/{total_batches}")
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
                print(Fore.GREEN + f"\n✅ Batch {batch_num} completed successfully!")
                print(Fore.GREEN + f"   Output: {output_path}")
            else:
                print(Fore.RED + f"\n❌ Batch {batch_num} failed!")
                if 'error' in result:
                    print(Fore.RED + f"   Error: {result['error']}")
        
        except Exception as e:
            print(Fore.RED + f"\n❌ Error migrating batch {batch_num}: {e}")
            results.append({
                'batch': batch_num,
                'success': False,
                'error': str(e)
            })
    
    # Summary
    print("\n" + "="*80)
    print(Fore.CYAN + "MIGRATION SUMMARY")
    print("="*80)
    
    successful = [r for r in results if r.get('success')]
    failed = [r for r in results if not r.get('success')]
    
    print(f"\n✅ Successful batches: {len(successful)}/{total_batches}")
    for r in successful:
        stats = r.get('stats', {})
        print(f"   Batch {r['batch']}: {stats.get('total_rows', 0):,} rows")
    
    if failed:
        print(f"\n❌ Failed batches: {len(failed)}/{total_batches}")
        for r in failed:
            print(f"   Batch {r['batch']}: {r.get('error', 'Unknown error')}")
    
    print("\n" + "="*80)
    print(Fore.GREEN + "MIGRATION COMPLETE!")
    print("="*80)

if __name__ == "__main__":
    main()

