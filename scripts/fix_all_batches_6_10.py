import pandas as pd
import numpy as np
from pathlib import Path

def aggressive_fix_batch(batch_file: str):
    """Aggressively fix a batch file by creating parent rows for all orphaned handles."""
    print(f"\n{'='*80}")
    print(f"FIXING: {Path(batch_file).name}")
    print(f"{'='*80}")
    
    df = pd.read_csv(batch_file, dtype=str, keep_default_na=False)
    original_count = len(df)
    
    # Clean whitespace
    cols_to_clean = ['Title', 'Handle', 'Option1 Value', 'Option1 Name']
    for col in cols_to_clean:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    
    # Identify parents vs variants
    df['is_parent'] = (df['Title'] != '') & (df['Title'].str.lower() != 'nan')
    
    # Group by Handle
    grouped = df.groupby('Handle')
    
    valid_rows = []
    fixed_groups = 0
    
    for handle, group in grouped:
        if not handle or str(handle).lower() == 'nan':
            continue
        
        parents = group[group['is_parent']]
        variants = group[~group['is_parent']]
        
        # Case A: Has parent
        if not parents.empty:
            parent_row = parents.iloc[0].copy()
            valid_rows.append(parent_row)
            
            # Add all variants
            for _, variant in variants.iterrows():
                if not variant['Option1 Value']:
                    if variant.get('Variant SKU'):
                        variant['Option1 Value'] = str(variant['Variant SKU'])
                    else:
                        variant['Option1 Value'] = "Default"
                
                if not variant['Option1 Name']:
                    variant['Option1 Name'] = "Size"
                
                valid_rows.append(variant)
        
        # Case B: Orphaned variants - create parent
        elif not variants.empty:
            first_variant = variants.iloc[0]
            new_parent = first_variant.copy()
            
            # Create title from handle
            new_title = handle.replace('-', ' ').title()
            new_parent['Title'] = new_title
            new_parent['is_parent'] = True
            
            # Clear variant fields for parent
            new_parent['Variant SKU'] = ''
            new_parent['Variant Price'] = ''
            new_parent['Option1 Value'] = ''
            
            # Set Option1 Name
            opt1_name = first_variant.get('Option1 Name', '')
            if not opt1_name or opt1_name.lower() == 'nan':
                opt1_name = "Size"
            new_parent['Option1 Name'] = opt1_name
            
            valid_rows.append(new_parent)
            
            # Add variants
            for _, variant in variants.iterrows():
                if not variant['Option1 Value']:
                    if variant.get('Variant SKU'):
                        variant['Option1 Value'] = str(variant['Variant SKU'])
                    else:
                        variant['Option1 Value'] = "Default"
                
                if not variant['Option1 Name']:
                    variant['Option1 Name'] = opt1_name
                
                valid_rows.append(variant)
            
            fixed_groups += 1
    
    # Reconstruct DataFrame
    final_df = pd.DataFrame(valid_rows)
    
    # Drop helper column
    if 'is_parent' in final_df.columns:
        final_df = final_df.drop(columns=['is_parent'])
    
    # Final safety check
    for col in final_df.columns:
        final_df[col] = final_df[col].fillna('').astype(str)
    
    error_mask = (final_df['Title'] == '') & (final_df['Option1 Value'] == '')
    if error_mask.any():
        final_df.loc[error_mask, 'Option1 Value'] = 'Default'
    
    final_count = len(final_df)
    print(f"Original rows: {original_count:,}")
    print(f"Fixed orphaned groups: {fixed_groups}")
    print(f"Final rows: {final_count:,}")
    
    # Overwrite file
    final_df.to_csv(batch_file, index=False)
    print(f"✅ Fixed and saved: {batch_file}")
    
    return fixed_groups

def main():
    print("="*80)
    print("FIXING ALL BATCHES 6-10")
    print("="*80)
    
    batch_files = [
        'data/output/shopify_products_batch_6_of_10.csv',
        'data/output/shopify_products_batch_7_of_10.csv',
        'data/output/shopify_products_batch_8_of_10.csv',
        'data/output/shopify_products_batch_9_of_10.csv',
        'data/output/shopify_products_batch_10_of_10.csv',
    ]
    
    total_fixed = 0
    for batch_file in batch_files:
        if Path(batch_file).exists():
            fixed = aggressive_fix_batch(batch_file)
            total_fixed += fixed
        else:
            print(f"\n⚠️  File not found: {batch_file}")
    
    print(f"\n{'='*80}")
    print(f"TOTAL: Fixed {total_fixed} orphaned product groups across all batches")
    print(f"{'='*80}")
    
    # Verify
    print("\n" + "="*80)
    print("VERIFICATION")
    print("="*80)
    
    for batch_file in batch_files:
        if not Path(batch_file).exists():
            continue
        
        df = pd.read_csv(batch_file, dtype=str, keep_default_na=False)
        parent_handles = set(df[df['Title'].astype(str).str.strip() != '']['Handle'].astype(str).str.strip().unique())
        variant_handles = set(df[df['Title'].astype(str).str.strip() == '']['Handle'].astype(str).str.strip().unique())
        orphaned = variant_handles - parent_handles
        
        batch_num = Path(batch_file).stem.split('_')[3]
        if orphaned:
            print(f"Batch {batch_num}: ❌ Still has {len(orphaned)} orphaned handles")
        else:
            print(f"Batch {batch_num}: ✅ No orphaned handles!")

if __name__ == "__main__":
    main()

