#!/usr/bin/env python3
"""
Fix Batch 5 CSV to ensure no 'Title can't be blank' errors.
Ensures all variant rows have proper Option1 Name and Option1 Value.
"""

import pandas as pd
import sys
from pathlib import Path

def fix_batch5_titles():
    """Fix batch 5 CSV file to prevent 'Title can't be blank' errors."""
    
    csv_path = Path('data/output/shopify_products_batch_5_of_5.csv')
    
    if not csv_path.exists():
        print(f"ERROR: File not found: {csv_path}")
        return False
    
    print("=" * 80)
    print("FIXING BATCH 5 CSV FOR 'Title can't be blank' ERROR")
    print("=" * 80)
    
    # Read CSV with keep_default_na=False to preserve empty strings
    df = pd.read_csv(csv_path, low_memory=False, keep_default_na=False)
    
    print(f"\nTotal rows: {len(df):,}")
    
    # Ensure all columns are strings
    for col in df.columns:
        df[col] = df[col].astype(str)
    
    fixes_applied = 0
    rows_removed = []
    
    # Process each row
    for idx in df.index:
        title = str(df.at[idx, 'Title']).strip()
        handle = str(df.at[idx, 'Handle']).strip()
        option1_name = str(df.at[idx, 'Option1 Name']).strip()
        option1_value = str(df.at[idx, 'Option1 Value']).strip()
        
        # If title is empty, this is a variant row
        if not title or title == '' or title.lower() == 'nan':
            # Variant row MUST have:
            # 1. Handle (to link to parent)
            # 2. Option1 Name
            # 3. Option1 Value
            
            needs_fix = False
            
            # Ensure Handle is set
            if not handle or handle == '' or handle.lower() == 'nan':
                # Try to find handle from other rows (shouldn't happen, but just in case)
                print(f"  WARNING: Row {idx + 2} has blank title but no Handle - will remove")
                rows_removed.append(idx)
                continue
            
            # Ensure Option1 Name is set
            if not option1_name or option1_name == '' or option1_name.lower() == 'nan':
                df.at[idx, 'Option1 Name'] = 'Size'
                needs_fix = True
                fixes_applied += 1
            
            # Ensure Option1 Value is set
            if not option1_value or option1_value == '' or option1_value.lower() == 'nan':
                # Try to get from another variant with same handle
                handle_rows = df[df['Handle'] == handle]
                other_variants = handle_rows[
                    (handle_rows.index != idx) &
                    (handle_rows['Title'].astype(str).str.strip() == '') &
                    (handle_rows['Option1 Value'].astype(str).str.strip() != '')
                ]
                
                if len(other_variants) > 0:
                    # Copy Option1 Value from another variant
                    df.at[idx, 'Option1 Value'] = str(other_variants.iloc[0]['Option1 Value']).strip()
                    needs_fix = True
                    fixes_applied += 1
                else:
                    # Use Variant SKU or generate default
                    variant_sku = str(df.at[idx, 'Variant SKU']).strip()
                    if variant_sku and variant_sku != '' and variant_sku.lower() != 'nan':
                        df.at[idx, 'Option1 Value'] = variant_sku
                    else:
                        df.at[idx, 'Option1 Value'] = f"Variant-{handle}"
                    needs_fix = True
                    fixes_applied += 1
            
            # Ensure Title is completely empty (not even a space)
            if title != '':
                df.at[idx, 'Title'] = ''
                needs_fix = True
                fixes_applied += 1
        else:
            # Parent row - ensure Title is not empty
            if not title or title == '' or title.lower() == 'nan':
                # This shouldn't happen, but if it does, remove the row
                print(f"  WARNING: Row {idx + 2} is marked as parent but has blank title - will remove")
                rows_removed.append(idx)
                continue
    
    # Remove problematic rows
    if rows_removed:
        df = df.drop(rows_removed)
        print(f"\nRemoved {len(rows_removed)} problematic rows")
    
    # Replace any 'nan' strings with empty strings
    df = df.replace('nan', '')
    df = df.replace('NaN', '')
    df = df.replace('None', '')
    
    # Ensure all empty values are truly empty strings
    for col in df.columns:
        df[col] = df[col].apply(lambda x: '' if str(x).strip().lower() in ['nan', 'none', 'null'] else str(x))
    
    # Write the fixed CSV
    backup_path = csv_path.with_suffix('.csv.backup')
    if backup_path.exists():
        backup_path.unlink()
    csv_path.rename(backup_path)
    print(f"\nCreated backup: {backup_path}")
    
    # Write with explicit handling of empty strings
    df.to_csv(
        csv_path,
        index=False,
        encoding='utf-8',
        lineterminator='\n',
        quoting=1,  # QUOTE_ALL
        na_rep=''  # Write empty string for NaN
    )
    
    print(f"\n✅ Fixed CSV written to: {csv_path}")
    print(f"   Applied {fixes_applied} fixes")
    print(f"   Removed {len(rows_removed)} rows")
    print(f"   Final row count: {len(df):,}")
    
    # Verify the fix
    print("\n" + "=" * 80)
    print("VERIFYING FIXED CSV")
    print("=" * 80)
    
    df_check = pd.read_csv(csv_path, low_memory=False, keep_default_na=False)
    variant_rows = df_check[df_check['Title'].astype(str).str.strip() == '']
    
    issues = 0
    for idx, row in variant_rows.iterrows():
        if (not str(row.get('Handle', '')).strip() or
            not str(row.get('Option1 Name', '')).strip() or
            not str(row.get('Option1 Value', '')).strip()):
            issues += 1
            if issues <= 5:
                print(f"  Row {idx + 2} still has issues")
    
    if issues == 0:
        print("✅ All variant rows are valid!")
    else:
        print(f"⚠️ Found {issues} rows that still need attention")
    
    return True

if __name__ == '__main__':
    try:
        success = fix_batch5_titles()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

