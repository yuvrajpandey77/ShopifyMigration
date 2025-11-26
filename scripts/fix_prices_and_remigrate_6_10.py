#!/usr/bin/env python3
"""
Fix zero prices in batches 6-10 by getting prices from source data, then re-migrate.
"""

import sys
import pandas as pd
from pathlib import Path
from colorama import Fore, Style, init

init(autoreset=True)

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.migrate_batches import migrate_batch, load_config

def get_price_from_source(row: pd.Series) -> str:
    """Extract price from source row."""
    for field in ['Regular price', 'Sale price', 'Price']:
        if field in row and pd.notna(row[field]):
            try:
                price_str = str(row[field]).replace('$', '').replace(',', '').replace('₹', '').strip()
                if price_str and price_str.lower() != 'nan':
                    price = float(price_str)
                    if price > 0:
                        return f"{price:.2f}"
            except:
                continue
    return None

def fix_prices_from_source(batch_num: int):
    """Fix prices in a batch by looking up source data."""
    batch_file = f'data/output/shopify_products_batch_{batch_num}_of_10.csv'
    source_batch = f'data/temp_batches/batch_{batch_num}_source.csv'
    
    if not Path(batch_file).exists() or not Path(source_batch).exists():
        return False
    
    print(f"\n{'='*80}")
    print(f"FIXING PRICES FOR BATCH {batch_num}")
    print(f"{'='*80}")
    
    # Read both files
    df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
    source_df = pd.read_csv(source_batch, dtype=str, keep_default_na=False, low_memory=False)
    
    # Create mapping from handle to source data
    handle_to_source = {}
    for _, source_row in source_df.iterrows():
        # Try to match by SKU or Name
        sku = str(source_row.get('SKU', '')).strip()
        name = str(source_row.get('Name', '')).strip()
        
        # Get price from source
        price = get_price_from_source(source_row)
        if price:
            # Store by SKU and Name
            if sku and sku.lower() != 'nan':
                handle_to_source[sku] = {'price': price, 'row': source_row}
            if name and name.lower() != 'nan':
                handle_to_source[name] = {'price': price, 'row': source_row}
    
    # Fix parent rows with zero prices
    parent_rows = df[df['Title'].astype(str).str.strip() != '']
    fixed_count = 0
    
    for idx, parent_row in parent_rows.iterrows():
        handle = str(parent_row['Handle']).strip()
        title = str(parent_row['Title']).strip()
        
        # Check if price is zero or missing
        price_fields = ['Price', 'Variant Price']
        has_valid_price = False
        
        for field in price_fields:
            if field in parent_row:
                price_str = str(parent_row[field]).strip()
                if price_str and price_str.lower() != 'nan':
                    try:
                        price = float(price_str.replace('$', '').replace(',', '').strip())
                        if price > 0:
                            has_valid_price = True
                            break
                    except:
                        pass
        
        if not has_valid_price:
            # Try to find price from source
            price = None
            
            # Try by handle (convert to product name)
            product_name = handle.replace('-', ' ').title()
            if product_name in handle_to_source:
                price = handle_to_source[product_name]['price']
            elif handle in handle_to_source:
                price = handle_to_source[handle]['price']
            else:
                # Try to find in source by matching name
                matching_source = source_df[source_df['Name'].astype(str).str.contains(title, case=False, na=False)]
                if not matching_source.empty:
                    price = get_price_from_source(matching_source.iloc[0])
            
            if price:
                # Set price in all price fields
                for field in price_fields:
                    if field in df.columns:
                        df.loc[idx, field] = price
                fixed_count += 1
                print(f"  Fixed: {title} -> ${price}")
            else:
                print(f"  ⚠️  Could not find price for: {title}")
    
    if fixed_count > 0:
        df.to_csv(batch_file, index=False)
        print(f"\n✅ Fixed {fixed_count} parent rows with prices")
        return True
    else:
        print(f"\n⚠️  No prices fixed (all already have prices or source data unavailable)")
        return False

def main():
    """Fix prices and re-migrate batches 6-10."""
    
    config = load_config()
    shopify_template = config.get('files', {}).get('shopify_template', '/home/yuvraj/Documents/product_template_csv_unit_price.csv')
    output_dir = Path(config.get('files', {}).get('output_dir', './data/output'))
    mapping_config = 'config/field_mapping.json'
    temp_dir = Path('data/temp_batches')
    
    print(Fore.CYAN + "="*80)
    print(Fore.CYAN + "FIXING PRICES AND RE-MIGRATING BATCHES 6-10")
    print(Fore.CYAN + "="*80)
    
    # Re-migrate from source (this will properly set prices)
    print(Fore.YELLOW + "\nRe-migrating batches 6-10 from source to fix prices...")
    
    results = []
    
    for batch_num in range(6, 11):
        batch_source = temp_dir / f"batch_{batch_num}_source.csv"
        
        if not batch_source.exists():
            print(Fore.YELLOW + f"⚠️  Batch {batch_num} source file not found: {batch_source}")
            continue
        
        output_path = output_dir / f"shopify_products_batch_{batch_num}_of_10.csv"
        
        try:
            print(Fore.GREEN + f"\n{'='*80}")
            print(Fore.GREEN + f"Re-migrating Batch {batch_num}/10")
            print(Fore.GREEN + f"{'='*80}")
            
            result = migrate_batch(
                batch_source_csv=str(batch_source),
                batch_num=batch_num,
                shopify_template=shopify_template,
                output_path=str(output_path),
                mapping_config=mapping_config,
                total_batches=10
            )
            
            results.append({
                'batch': batch_num,
                'success': result.get('success', False),
                'output_file': str(output_path),
                'stats': result.get('stats', {})
            })
            
            if result.get('success'):
                print(Fore.GREEN + f"\n✅ Batch {batch_num} re-migrated!")
                
                # Verify no orphaned variants and check prices
                df = pd.read_csv(output_path, dtype=str, keep_default_na=False, low_memory=False)
                parent_rows = df[df['Title'].astype(str).str.strip() != '']
                
                # Check for orphaned variants
                parent_handles = set(parent_rows['Handle'].astype(str).str.strip().unique())
                variant_handles = set(df[df['Title'].astype(str).str.strip() == '']['Handle'].astype(str).str.strip().unique())
                orphaned = variant_handles - parent_handles
                
                # Check for zero prices
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
                    print(Fore.YELLOW + f"  ⚠️  Found {len(orphaned)} orphaned handles - applying fix...")
                    # Apply aggressive fix
                    from scripts.fix_all_batches_6_10 import aggressive_fix_batch
                    aggressive_fix_batch(str(output_path))
                    print(Fore.GREEN + f"  ✅ Fixed!")
                
                if zero_price_count > 0:
                    print(Fore.YELLOW + f"  ⚠️  Found {zero_price_count} parent rows with zero prices")
                    # Try to fix from source
                    if fix_prices_from_source(batch_num):
                        print(Fore.GREEN + f"  ✅ Fixed prices from source!")
                    else:
                        print(Fore.RED + f"  ❌ Could not fix prices - may need manual review")
                else:
                    print(Fore.GREEN + f"  ✅ Verified: All parent rows have valid prices!")
                
                if not orphaned and zero_price_count == 0:
                    print(Fore.GREEN + f"  ✅✅✅ Batch {batch_num} is perfect!")
        
        except Exception as e:
            print(Fore.RED + f"\n❌ Error re-migrating batch {batch_num}: {e}")
            results.append({
                'batch': batch_num,
                'success': False,
                'error': str(e)
            })
    
    # Summary
    print("\n" + "="*80)
    print(Fore.CYAN + "RE-MIGRATION SUMMARY")
    print("="*80)
    
    successful = [r for r in results if r.get('success')]
    failed = [r for r in results if not r.get('success')]
    
    print(f"\n✅ Successful batches: {len(successful)}/5")
    for r in successful:
        stats = r.get('stats', {})
        print(f"   Batch {r['batch']}: {stats.get('total_rows', 0):,} rows")
    
    if failed:
        print(f"\n❌ Failed batches: {len(failed)}/5")
        for r in failed:
            print(f"   Batch {r['batch']}: {r.get('error', 'Unknown error')}")
    
    print("\n" + "="*80)
    print(Fore.GREEN + "RE-MIGRATION COMPLETE!")
    print("="*80)

if __name__ == "__main__":
    main()

