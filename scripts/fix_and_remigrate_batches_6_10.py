#!/usr/bin/env python3
"""
Fix batches 6-10 by creating parent rows for orphaned variants, then re-migrate.
"""

import sys
from pathlib import Path
from colorama import Fore, Style, init
import pandas as pd

# Initialize colorama
init(autoreset=True)

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.migrate_batches import migrate_batch, load_config

def fix_orphaned_variants(batch_file: str) -> str:
    """Fix orphaned variants by creating parent rows."""
    print(f"\n{'='*80}")
    print(f"FIXING ORPHANED VARIANTS: {Path(batch_file).name}")
    print(f"{'='*80}")
    
    df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
    original_count = len(df)
    
    # Identify parent vs variant rows
    df['is_parent'] = df['Title'].astype(str).str.strip() != ''
    
    # Group by Handle
    grouped = df.groupby('Handle')
    
    valid_rows = []
    fixed_count = 0
    
    for handle, group in grouped:
        if not handle or str(handle).lower() == 'nan':
            continue
        
        parents = group[group['is_parent']]
        variants = group[~group['is_parent']]
        
        if not parents.empty:
            # Has parent - add parent and all variants
            valid_rows.append(parents.iloc[0].copy())
            for _, variant in variants.iterrows():
                valid_rows.append(variant)
        elif not variants.empty:
            # Orphaned variants - create parent row
            first_variant = variants.iloc[0]
            new_parent = first_variant.copy()
            
            # Create title from handle
            new_title = handle.replace('-', ' ').title()
            new_parent['Title'] = new_title
            new_parent['is_parent'] = True
            
            # Clear variant-specific fields for parent
            new_parent['Variant SKU'] = ''
            new_parent['Variant Price'] = ''
            new_parent['Option1 Value'] = ''
            
            # Set Option1 Name if variants have it
            opt1_name = first_variant.get('Option1 Name', '')
            if not opt1_name or str(opt1_name).strip() == '':
                opt1_name = 'Size'
            new_parent['Option1 Name'] = opt1_name
            
            valid_rows.append(new_parent)
            
            # Add variants
            for _, variant in variants.iterrows():
                # Ensure Option1 Value is set
                if not variant.get('Option1 Value') or str(variant.get('Option1 Value', '')).strip() == '':
                    if variant.get('Variant SKU'):
                        variant['Option1 Value'] = variant['Variant SKU']
                    else:
                        variant['Option1 Value'] = 'Default'
                
                if not variant.get('Option1 Name') or str(variant.get('Option1 Name', '')).strip() == '':
                    variant['Option1 Name'] = opt1_name
                
                valid_rows.append(variant)
            
            fixed_count += 1
    
    # Reconstruct DataFrame
    final_df = pd.DataFrame(valid_rows)
    
    # Drop helper column
    if 'is_parent' in final_df.columns:
        final_df = final_df.drop(columns=['is_parent'])
    
    # Final safety check: ensure no blank titles with empty Option1 Value
    error_mask = (final_df['Title'] == '') & (final_df['Option1 Value'] == '')
    if error_mask.any():
        final_df.loc[error_mask, 'Option1 Value'] = 'Default'
    
    print(f"  Fixed {fixed_count} orphaned product groups")
    print(f"  Original rows: {original_count:,} -> Final rows: {len(final_df):,}")
    
    # Overwrite original file
    final_df.to_csv(batch_file, index=False)
    print(f"  ✅ Fixed and saved: {batch_file}")
    
    return batch_file

def main():
    """Fix and re-migrate batches 6-10."""
    
    config = load_config()
    shopify_template = config.get('files', {}).get('shopify_template', '/home/yuvraj/Documents/product_template_csv_unit_price.csv')
    output_dir = Path(config.get('files', {}).get('output_dir', './data/output'))
    mapping_config = 'config/field_mapping.json'
    temp_dir = Path('data/temp_batches')
    
    batches_to_fix = [6, 7, 8, 9, 10]
    
    print(Fore.CYAN + "="*80)
    print(Fore.CYAN + "FIXING AND RE-MIGRATING BATCHES 6-10")
    print(Fore.CYAN + "="*80)
    
    # Step 1: Fix orphaned variants in existing CSV files
    print(Fore.YELLOW + "\nSTEP 1: Fixing orphaned variants...")
    fixed_files = []
    
    for batch_num in batches_to_fix:
        batch_file = output_dir / f"shopify_products_batch_{batch_num}_of_10.csv"
        if batch_file.exists():
            try:
                fixed_file = fix_orphaned_variants(str(batch_file))
                fixed_files.append((batch_num, fixed_file))
            except Exception as e:
                print(Fore.RED + f"  ❌ Error fixing batch {batch_num}: {e}")
    
    # Step 2: Re-migrate from source (better approach - regenerate from source)
    print(Fore.YELLOW + "\n\nSTEP 2: Re-migrating from source files...")
    
    results = []
    
    for batch_num in batches_to_fix:
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
                print(Fore.GREEN + f"\n✅ Batch {batch_num} re-migrated successfully!")
                
                # Verify no orphaned variants
                df = pd.read_csv(output_path, dtype=str, keep_default_na=False, low_memory=False)
                parent_handles = set(df[df['Title'].astype(str).str.strip() != '']['Handle'].astype(str).str.strip().unique())
                variant_handles = set(df[df['Title'].astype(str).str.strip() == '']['Handle'].astype(str).str.strip().unique())
                orphaned = variant_handles - parent_handles
                
                if orphaned:
                    print(Fore.YELLOW + f"  ⚠️  Still found {len(orphaned)} orphaned handles - applying fix...")
                    fix_orphaned_variants(str(output_path))
                    print(Fore.GREEN + f"  ✅ Fixed!")
                else:
                    print(Fore.GREEN + f"  ✅ Verified: No orphaned variants!")
            else:
                print(Fore.RED + f"\n❌ Batch {batch_num} failed!")
        
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

