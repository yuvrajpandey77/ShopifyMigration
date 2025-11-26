import pandas as pd
from pathlib import Path

def fix_parent_prices(batch_file: str, source_batch_file: str = None):
    """Fix parent rows that have zero prices by getting prices from variants or source."""
    print(f"\n{'='*80}")
    print(f"FIXING PARENT PRICES: {Path(batch_file).name}")
    print(f"{'='*80}")
    
    df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
    
    # Load source data if available
    source_df = None
    if source_batch_file and Path(source_batch_file).exists():
        source_df = pd.read_csv(source_batch_file, dtype=str, keep_default_na=False, low_memory=False)
        print(f"Loaded source data: {len(source_df)} rows")
    
    # Get parent rows
    parent_rows = df[df['Title'].astype(str).str.strip() != '']
    fixed_count = 0
    
    for idx, parent_row in parent_rows.iterrows():
        handle = str(parent_row['Handle']).strip()
        title = str(parent_row['Title']).strip()
        
        # Check if price is zero or missing
        price_fields = ['Price', 'Variant Price']
        has_valid_price = False
        current_price = None
        
        for field in price_fields:
            if field in parent_row:
                price_str = str(parent_row[field]).strip()
                if price_str and price_str.lower() != 'nan':
                    try:
                        price = float(price_str.replace('$', '').replace(',', '').strip())
                        if price > 0:
                            has_valid_price = True
                            current_price = price
                            break
                    except:
                        pass
        
        if not has_valid_price:
            # Try to get price from variants of the same handle
            variant_rows = df[(df['Handle'] == handle) & (df['Title'].astype(str).str.strip() == '')]
            
            price = None
            for _, variant in variant_rows.iterrows():
                variant_price_fields = ['Variant Price', 'Price']
                for field in variant_price_fields:
                    if field in variant:
                        price_str = str(variant[field]).strip()
                        if price_str and price_str.lower() != 'nan':
                            try:
                                p = float(price_str.replace('$', '').replace(',', '').strip())
                                if p > 0:
                                    price = p
                                    break
                            except:
                                pass
                if price:
                    break
            
            # If still no price, try source data
            if not price and source_df is not None:
                # Try to match by name
                matching = source_df[source_df['Name'].astype(str).str.contains(title, case=False, na=False)]
                if matching.empty:
                    # Try by handle converted to name
                    product_name = handle.replace('-', ' ').title()
                    matching = source_df[source_df['Name'].astype(str).str.contains(product_name, case=False, na=False)]
                
                if not matching.empty:
                    for _, source_row in matching.iterrows():
                        for field in ['Regular price', 'Sale price', 'Price']:
                            if field in source_row:
                                try:
                                    price_str = str(source_row[field]).replace('$', '').replace(',', '').replace('₹', '').strip()
                                    if price_str and price_str.lower() != 'nan':
                                        p = float(price_str)
                                        if p > 0:
                                            price = p
                                            break
                                except:
                                    pass
                        if price:
                            break
            
            if price:
                # Set price in all price fields
                price_str = f"{price:.2f}"
                for field in price_fields:
                    if field in df.columns:
                        df.loc[idx, field] = price_str
                fixed_count += 1
                print(f"  ✅ Fixed: {title} -> ${price_str}")
            else:
                print(f"  ⚠️  Could not find price for: {title}")
    
    if fixed_count > 0:
        df.to_csv(batch_file, index=False)
        print(f"\n✅ Fixed {fixed_count} parent rows with prices")
    else:
        print(f"\n✅ All parent rows already have valid prices")
    
    return fixed_count

def main():
    print("="*80)
    print("FIXING PARENT ROW PRICES IN BATCHES 6-10")
    print("="*80)
    
    total_fixed = 0
    
    for i in range(6, 11):
        batch_file = f'data/output/shopify_products_batch_{i}_of_10.csv'
        source_batch = f'data/temp_batches/batch_{i}_source.csv'
        
        if Path(batch_file).exists():
            fixed = fix_parent_prices(batch_file, source_batch if Path(source_batch).exists() else None)
            total_fixed += fixed
    
    print(f"\n{'='*80}")
    print(f"TOTAL: Fixed {total_fixed} parent rows with prices")
    print(f"{'='*80}")
    
    # Final verification
    print("\n" + "="*80)
    print("FINAL VERIFICATION")
    print("="*80)
    
    all_good = True
    for i in range(6, 11):
        batch_file = f'data/output/shopify_products_batch_{i}_of_10.csv'
        if not Path(batch_file).exists():
            continue
        
        df = pd.read_csv(batch_file, dtype=str, keep_default_na=False, low_memory=False)
        parent_rows = df[df['Title'].astype(str).str.strip() != '']
        
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
        
        print(f"Batch {i}: {zero_price_count} parent rows with zero price")
        if zero_price_count > 0:
            all_good = False
    
    if all_good:
        print("\n✅✅✅ ALL BATCHES 6-10 HAVE VALID PRICES! ✅✅✅")
    else:
        print("\n⚠️  Some batches still have zero prices - may need manual review")

if __name__ == "__main__":
    main()

