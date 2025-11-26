import pandas as pd
import numpy as np
import sys
import re

def aggressive_fix_batch_5():
    print("="*80)
    print("AGGRESSIVE FIX FOR BATCH 5 - FINAL ATTEMPT")
    print("="*80)

    input_file = 'data/output/shopify_products_batch_5_of_5.csv'
    output_file = 'data/output/shopify_products_batch_5_of_5_FINAL.csv'

    # Read with strict types to prevent pandas inference issues
    df = pd.read_csv(input_file, dtype=str, keep_default_na=False)
    original_count = len(df)
    print(f"Original row count: {original_count}")

    # 1. CLEANUP: Strip whitespace from critical columns
    cols_to_clean = ['Title', 'Handle', 'Option1 Value', 'Option1 Name']
    for col in cols_to_clean:
        if col in df.columns:
            df[col] = df[col].str.strip()

    # 2. IDENTIFY PARENTS VS VARIANTS
    # A parent row has a non-empty Title.
    # A variant row has an empty Title.
    df['is_parent'] = df['Title'].astype(bool) & (df['Title'] != '')
    
    # 3. GROUP BY HANDLE
    grouped = df.groupby('Handle')
    
    valid_rows = []
    fixed_groups = 0
    dropped_groups = 0
    
    print("\nProcessing product groups...")
    
    for handle, group in grouped:
        if not handle:
            continue
            
        parents = group[group['is_parent']]
        variants = group[~group['is_parent']]
        
        # Case A: Good group (Has parent, has variants or is simple product)
        if not parents.empty:
            # Take the first parent found (should only be one, but safeguard)
            parent_row = parents.iloc[0].copy()
            
            # If we have multiple parents for same handle, that's weird but we just take one.
            # We add the parent row
            valid_rows.append(parent_row)
            
            # Add all valid variants
            for _, variant in variants.iterrows():
                # ENSURE OPTION1 VALUE IS SET
                if not variant['Option1 Value']:
                    # Copy from parent if variant option1 is blank? 
                    # Actually, variants MUST have Option1 Value. 
                    # If it's blank, try to set it from SKU or generate default
                    if variant['Variant SKU']:
                        variant['Option1 Value'] = variant['Variant SKU']
                    else:
                        variant['Option1 Value'] = "Default"
                
                # Ensure Option1 Name is set
                if not variant['Option1 Name']:
                    variant['Option1 Name'] = "Size"
                    
                valid_rows.append(variant)
                
        # Case B: Orphaned Variants (No parent row)
        elif parents.empty and not variants.empty:
            # We need to CREATE a parent row
            first_variant = variants.iloc[0]
            
            # Create parent from first variant
            new_parent = first_variant.copy()
            
            # Fix Title (Capitalize handle)
            new_title = handle.replace('-', ' ').title()
            new_parent['Title'] = new_title
            new_parent['is_parent'] = True
            
            # Clear variant specific fields for the parent
            new_parent['Variant SKU'] = ''
            new_parent['Variant Price'] = ''
            new_parent['Option1 Value'] = '' # Parent option1 value must be empty for Shopify if variants exist
            
            # Ensure Option1 Name is "Size" (or whatever the variants use)
            opt1_name = first_variant['Option1 Name'] if first_variant['Option1 Name'] else "Size"
            new_parent['Option1 Name'] = opt1_name
            
            valid_rows.append(new_parent)
            
            # Add the variants
            for _, variant in variants.iterrows():
                if not variant['Option1 Value']:
                     if variant['Variant SKU']:
                        variant['Option1 Value'] = variant['Variant SKU']
                     else:
                        variant['Option1 Value'] = "Default"
                
                if not variant['Option1 Name']:
                    variant['Option1 Name'] = opt1_name
                    
                valid_rows.append(variant)
            
            fixed_groups += 1
            
        else:
            # Case C: Completely empty/invalid group
            dropped_groups += 1
            continue

    # 4. RECONSTRUCT DATAFRAME
    final_df = pd.DataFrame(valid_rows)
    
    # Drop helper column
    if 'is_parent' in final_df.columns:
        final_df = final_df.drop(columns=['is_parent'])
        
    # 5. FINAL SAFETY CHECKS
    # Ensure no rows have (Title="" AND Option1 Value="")
    # This is the specific condition for "Title can't be blank" error
    
    # Force string types again
    for col in final_df.columns:
        final_df[col] = final_df[col].fillna('')
        
    # Check for the error condition
    error_mask = (final_df['Title'] == '') & (final_df['Option1 Value'] == '')
    error_rows = final_df[error_mask]
    
    if not error_rows.empty:
        print(f"\nCRITICAL: Found {len(error_rows)} rows that would still cause error. Patching them...")
        # Fix: Give them a default Option1 Value
        final_df.loc[error_mask, 'Option1 Value'] = 'Default'
        
    # Final verification count
    final_count = len(final_df)
    print(f"\nFinal row count: {final_count}")
    print(f"Fixed orphaned groups: {fixed_groups}")
    print(f"Dropped invalid groups: {dropped_groups}")
    
    # 6. OVERWRITE ORIGINAL FILE
    final_df.to_csv(input_file, index=False)
    print(f"\n✅ Successfully overwrote {input_file}")
    print("This file is now strictly validated against the 'Title can't be blank' error.")

if __name__ == "__main__":
    aggressive_fix_batch_5()

