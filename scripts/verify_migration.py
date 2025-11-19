#!/usr/bin/env python3
"""
Comprehensive Migration Verification Script
Checks all critical aspects of the Shopify migration output.
"""

import pandas as pd
import sys
from pathlib import Path

def verify_migration(output_file: str):
    """Perform comprehensive verification of migration output."""
    
    print("=" * 80)
    print("COMPREHENSIVE MIGRATION VERIFICATION")
    print("=" * 80)
    print()
    
    try:
        df = pd.read_csv(output_file)
    except Exception as e:
        print(f"❌ ERROR: Could not read output file: {e}")
        return False
    
    print(f"Total rows in output: {len(df)}")
    print()
    
    # Get parent rows (rows with Title)
    parent_rows = df[pd.notna(df['Title']) & (df['Title'] != '')]
    variant_rows = df[pd.isna(df['Title']) | (df['Title'] == '')]
    
    print(f"Parent products: {len(parent_rows)}")
    print(f"Variant rows: {len(variant_rows)}")
    print()
    
    all_checks_passed = True
    
    # ========== 1. PRICE CHECKS ==========
    print("=" * 80)
    print("1. PRICE VERIFICATION")
    print("=" * 80)
    
    if 'Variant Price' in df.columns:
        # Check parent rows with variants (should have empty Variant Price)
        parents_with_variants = []
        for idx, parent in parent_rows.iterrows():
            handle = parent.get('Handle', '')
            if handle:
                variants = df[(df['Handle'] == handle) & (df.index != idx) & 
                             (pd.isna(df['Title']) | (df['Title'] == ''))]
                if len(variants) > 0:
                    parents_with_variants.append(idx)
        
        # Check variant rows have prices
        variant_prices = variant_rows['Variant Price'].notna() & (variant_rows['Variant Price'] != '')
        variant_prices_valid = variant_rows[variant_prices]
        
        # Check for zero prices
        zero_prices = []
        for idx, row in variant_prices_valid.iterrows():
            try:
                price_str = str(row['Variant Price']).replace('₹', '').replace(',', '').strip()
                price_val = float(price_str)
                if price_val <= 0:
                    zero_prices.append((idx, row.get('Handle', 'N/A'), price_val))
            except:
                pass
        
        # Check single products have prices
        single_products = [idx for idx in parent_rows.index if idx not in parents_with_variants]
        single_product_prices = []
        for idx in single_products:
            row = df.loc[idx]
            price = row.get('Variant Price', '')
            if not price or str(price).strip() == '':
                single_product_prices.append((idx, row.get('Title', 'N/A')))
            else:
                try:
                    price_str = str(price).replace('₹', '').replace(',', '').strip()
                    price_val = float(price_str)
                    if price_val <= 0:
                        zero_prices.append((idx, row.get('Handle', 'N/A'), price_val))
                except:
                    pass
        
        print(f"  ✓ Variant rows with prices: {len(variant_prices_valid)}/{len(variant_rows)}")
        print(f"  ✓ Zero prices found: {len(zero_prices)}")
        if zero_prices:
            print(f"    ⚠️  Products with zero price:")
            for idx, handle, price in zero_prices[:5]:
                print(f"      - {handle}: ${price}")
            all_checks_passed = False
        print(f"  ✓ Single products with prices: {len(single_products) - len(single_product_prices)}/{len(single_products)}")
        if single_product_prices:
            print(f"    ⚠️  Single products missing prices: {len(single_product_prices)}")
            all_checks_passed = False
    else:
        print("  ❌ Variant Price column not found")
        all_checks_passed = False
    
    print()
    
    # ========== 2. DESCRIPTION CHECKS ==========
    print("=" * 80)
    print("2. DESCRIPTION VERIFICATION")
    print("=" * 80)
    
    if 'Body (HTML)' in df.columns:
        parent_descriptions = parent_rows['Body (HTML)'].notna() & (parent_rows['Body (HTML)'] != '')
        missing_descriptions = parent_rows[~parent_descriptions]
        
        print(f"  ✓ Products with descriptions: {len(parent_rows[parent_descriptions])}/{len(parent_rows)}")
        
        if len(missing_descriptions) > 0:
            print(f"    ⚠️  Products missing descriptions: {len(missing_descriptions)}")
            all_checks_passed = False
        
        # Check tab styling
        if len(parent_rows[parent_descriptions]) > 0:
            sample = parent_rows[parent_descriptions].iloc[0]
            desc = str(sample['Body (HTML)'])
            has_tabs = 'tabs-container' in desc and 'tab active' in desc
            has_responsive = '@media' in desc and 'max-width' in desc
            has_js = 'addEventListener' in desc or 'querySelectorAll' in desc
            
            print(f"  ✓ Tab styling present: {has_tabs}")
            print(f"  ✓ Responsive design: {has_responsive}")
            print(f"  ✓ JavaScript functionality: {has_js}")
            
            if not (has_tabs and has_responsive and has_js):
                print(f"    ⚠️  Some descriptions missing styling features")
                all_checks_passed = False
    else:
        print("  ❌ Body (HTML) column not found")
        all_checks_passed = False
    
    print()
    
    # ========== 3. IMAGE CHECKS ==========
    print("=" * 80)
    print("3. IMAGE VERIFICATION")
    print("=" * 80)
    
    if 'Image Src' in df.columns:
        parent_images = parent_rows['Image Src'].notna() & (parent_rows['Image Src'] != '')
        missing_images = parent_rows[~parent_images]
        
        print(f"  ✓ Products with images: {len(parent_rows[parent_images])}/{len(parent_rows)}")
        
        if len(missing_images) > 0:
            print(f"    ⚠️  Products missing images: {len(missing_images)}")
            all_checks_passed = False
        
        # Check for multiple images (additional image rows)
        handles_with_images = df[df['Image Src'].notna() & (df['Image Src'] != '')]['Handle'].unique()
        handles_with_multiple = []
        for handle in handles_with_images:
            image_count = len(df[(df['Handle'] == handle) & (df['Image Src'].notna() & (df['Image Src'] != ''))])
            if image_count > 1:
                handles_with_multiple.append((handle, image_count))
        
        print(f"  ✓ Products with multiple images: {len(handles_with_multiple)}")
    else:
        print("  ❌ Image Src column not found")
        all_checks_passed = False
    
    print()
    
    # ========== 4. VARIANT CHECKS ==========
    print("=" * 80)
    print("4. VARIANT VERIFICATION")
    print("=" * 80)
    
    # Check variant rows have Option1 Value
    if 'Option1 Value' in df.columns:
        variant_options = variant_rows['Option1 Value'].notna() & (variant_rows['Option1 Value'] != '')
        missing_options = variant_rows[~variant_options]
        
        print(f"  ✓ Variants with Option1 Value: {len(variant_rows[variant_options])}/{len(variant_rows)}")
        
        if len(missing_options) > 0:
            print(f"    ⚠️  Variants missing Option1 Value: {len(missing_options)}")
            all_checks_passed = False
    
    # Check parent rows with variants have Option1 Name but empty Option1 Value
    if 'Option1 Name' in df.columns and 'Option1 Value' in df.columns:
        parents_with_variants_correct = 0
        parents_with_variants_incorrect = 0
        
        for idx, parent in parent_rows.iterrows():
            handle = parent.get('Handle', '')
            if handle:
                variants = df[(df['Handle'] == handle) & (df.index != idx) & 
                             (pd.isna(df['Title']) | (df['Title'] == ''))]
                if len(variants) > 0:
                    # Parent with variants should have Option1 Name but empty Option1 Value
                    option1_name = parent.get('Option1 Name', '')
                    option1_value = parent.get('Option1 Value', '')
                    # Check if Option1 Value is truly empty (handle NaN, empty string, 'nan' string)
                    is_empty = (pd.isna(option1_value) or str(option1_value).strip() == '' or str(option1_value).strip().lower() == 'nan')
                    if option1_name and is_empty:
                        parents_with_variants_correct += 1
                    else:
                        parents_with_variants_incorrect += 1
        
        print(f"  ✓ Parents with variants correctly configured: {parents_with_variants_correct}")
        if parents_with_variants_incorrect > 0:
            print(f"    ⚠️  Parents with variants incorrectly configured: {parents_with_variants_incorrect}")
            all_checks_passed = False
    
    # Check variant rows have empty Title
    variant_titles_empty = variant_rows[pd.isna(variant_rows['Title']) | (variant_rows['Title'] == '')]
    print(f"  ✓ Variant rows with empty Title: {len(variant_titles_empty)}/{len(variant_rows)}")
    
    if len(variant_titles_empty) != len(variant_rows):
        print(f"    ⚠️  Some variant rows have non-empty Title")
        all_checks_passed = False
    
    print()
    
    # ========== 5. INVENTORY CHECKS ==========
    print("=" * 80)
    print("5. INVENTORY VERIFICATION")
    print("=" * 80)
    
    if 'Variant Inventory Tracker' in df.columns and 'Variant Inventory Qty' in df.columns:
        # Check variants have inventory tracker set
        variant_trackers = variant_rows['Variant Inventory Tracker'].notna() & (variant_rows['Variant Inventory Tracker'] != '')
        variant_trackers_shopify = variant_rows[variant_rows['Variant Inventory Tracker'] == 'shopify']
        
        print(f"  ✓ Variants with inventory tracker: {len(variant_trackers_shopify)}/{len(variant_rows)}")
        
        # Check single products have inventory tracker
        single_product_trackers = []
        for idx in single_products:
            row = df.loc[idx]
            tracker = row.get('Variant Inventory Tracker', '')
            if tracker != 'shopify':
                single_product_trackers.append((idx, row.get('Title', 'N/A')))
        
        print(f"  ✓ Single products with inventory tracker: {len(single_products) - len(single_product_trackers)}/{len(single_products)}")
        if single_product_trackers:
            print(f"    ⚠️  Single products missing inventory tracker: {len(single_product_trackers)}")
            all_checks_passed = False
        
        # Check inventory quantities
        variant_qtys = variant_rows['Variant Inventory Qty'].notna() & (variant_rows['Variant Inventory Qty'] != '')
        variant_qtys_positive = []
        for idx in variant_rows[variant_qtys].index:
            row = df.loc[idx]
            try:
                qty = int(float(str(row['Variant Inventory Qty']).strip()))
                if qty > 0:
                    variant_qtys_positive.append(idx)
            except:
                pass
        
        print(f"  ✓ Variants with positive inventory: {len(variant_qtys_positive)}/{len(variant_rows)}")
    else:
        print("  ❌ Inventory columns not found")
        all_checks_passed = False
    
    print()
    
    # ========== 6. CATEGORY CHECKS ==========
    print("=" * 80)
    print("6. CATEGORY VERIFICATION")
    print("=" * 80)
    
    if 'Product category' in df.columns:
        parent_categories = parent_rows['Product category'].notna() & (parent_rows['Product category'] != '')
        missing_categories = parent_rows[~parent_categories]
        uncategorised = parent_rows[parent_rows['Product category'].str.contains('uncategorised|Uncategorised|uncategorized|Uncategorized', case=False, na=False)]
        
        print(f"  ✓ Products with categories: {len(parent_rows[parent_categories])}/{len(parent_rows)}")
        
        if len(missing_categories) > 0:
            print(f"    ⚠️  Products missing categories: {len(missing_categories)}")
            all_checks_passed = False
        
        if len(uncategorised) > 0:
            print(f"    ⚠️  Products with 'uncategorised' text: {len(uncategorised)}")
            all_checks_passed = False
        
        # Show category distribution
        if len(parent_rows[parent_categories]) > 0:
            unique_cats = parent_rows[parent_categories]['Product category'].unique()
            print(f"  ✓ Unique categories: {len(unique_cats)}")
    else:
        print("  ❌ Product category column not found")
        all_checks_passed = False
    
    print()
    
    # ========== 7. FULFILLMENT SERVICE CHECKS ==========
    print("=" * 80)
    print("7. FULFILLMENT SERVICE VERIFICATION")
    print("=" * 80)
    
    if 'Variant Fulfillment Service' in df.columns:
        # Check variants and single products have fulfillment service
        rows_with_variants = df[df['Variant Price'].notna() | df['Variant SKU'].notna()]
        fulfillment_set = rows_with_variants[rows_with_variants['Variant Fulfillment Service'] == 'manual']
        
        print(f"  ✓ Rows with fulfillment service set: {len(fulfillment_set)}/{len(rows_with_variants)}")
        
        missing_fulfillment = rows_with_variants[rows_with_variants['Variant Fulfillment Service'].isna() | 
                              (rows_with_variants['Variant Fulfillment Service'] == '')]
        if len(missing_fulfillment) > 0:
            print(f"    ⚠️  Rows missing fulfillment service: {len(missing_fulfillment)}")
            all_checks_passed = False
    else:
        print("  ❌ Variant Fulfillment Service column not found")
        all_checks_passed = False
    
    print()
    
    # ========== 8. INVENTORY POLICY CHECKS ==========
    print("=" * 80)
    print("8. INVENTORY POLICY VERIFICATION")
    print("=" * 80)
    
    if 'Variant Inventory Policy' in df.columns:
        rows_with_variants = df[df['Variant Price'].notna() | df['Variant SKU'].notna()]
        valid_policies = rows_with_variants[rows_with_variants['Variant Inventory Policy'].isin(['deny', 'continue'])]
        
        print(f"  ✓ Rows with valid inventory policy: {len(valid_policies)}/{len(rows_with_variants)}")
        
        invalid_policies = rows_with_variants[~rows_with_variants['Variant Inventory Policy'].isin(['deny', 'continue'])]
        if len(invalid_policies) > 0:
            print(f"    ⚠️  Rows with invalid inventory policy: {len(invalid_policies)}")
            all_checks_passed = False
    else:
        print("  ❌ Variant Inventory Policy column not found")
        all_checks_passed = False
    
    print()
    
    # ========== 9. SKU CHECKS ==========
    print("=" * 80)
    print("9. SKU VERIFICATION")
    print("=" * 80)
    
    if 'Variant SKU' in df.columns:
        variant_skus = variant_rows['Variant SKU'].notna() & (variant_rows['Variant SKU'] != '')
        single_product_skus = []
        for idx in single_products:
            row = df.loc[idx]
            sku = row.get('Variant SKU', '')
            if not sku or str(sku).strip() == '':
                single_product_skus.append((idx, row.get('Title', 'N/A')))
        
        print(f"  ✓ Variants with SKU: {len(variant_rows[variant_skus])}/{len(variant_rows)}")
        print(f"  ✓ Single products with SKU: {len(single_products) - len(single_product_skus)}/{len(single_products)}")
        
        if len(single_product_skus) > 0:
            print(f"    ⚠️  Single products missing SKU: {len(single_product_skus)}")
            all_checks_passed = False
        
        # Check for duplicate SKUs
        all_skus = df[df['Variant SKU'].notna() & (df['Variant SKU'] != '')]['Variant SKU']
        duplicate_skus = all_skus[all_skus.duplicated()]
        if len(duplicate_skus) > 0:
            print(f"    ⚠️  Duplicate SKUs found: {len(duplicate_skus)}")
            all_checks_passed = False
        else:
            print(f"  ✓ No duplicate SKUs")
    else:
        print("  ❌ Variant SKU column not found")
        all_checks_passed = False
    
    print()
    
    # ========== 10. HANDLE CHECKS ==========
    print("=" * 80)
    print("10. HANDLE VERIFICATION")
    print("=" * 80)
    
    if 'Handle' in df.columns:
        parent_handles = parent_rows['Handle'].notna() & (parent_rows['Handle'] != '')
        variant_handles = variant_rows['Handle'].notna() & (variant_rows['Handle'] != '')
        
        print(f"  ✓ Parent products with handles: {len(parent_rows[parent_handles])}/{len(parent_rows)}")
        print(f"  ✓ Variant rows with handles: {len(variant_rows[variant_handles])}/{len(variant_rows)}")
        
        # Check all variants share parent handle
        handles_with_variants = parent_rows[parent_handles]['Handle'].unique()
        all_variants_have_parent_handle = True
        for handle in handles_with_variants:
            variants = df[(df['Handle'] == handle) & (df.index != parent_rows[parent_rows['Handle'] == handle].index[0])]
            if len(variants) > 0:
                variant_handles_match = all(v.get('Handle', '') == handle for _, v in variants.iterrows())
                if not variant_handles_match:
                    all_variants_have_parent_handle = False
                    break
        
        if all_variants_have_parent_handle:
            print(f"  ✓ All variants share parent handle")
        else:
            print(f"    ⚠️  Some variants don't share parent handle")
            all_checks_passed = False
    else:
        print("  ❌ Handle column not found")
        all_checks_passed = False
    
    print()
    
    # ========== FINAL SUMMARY ==========
    print("=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    
    if all_checks_passed:
        print("✅ ALL CHECKS PASSED!")
        print("✅ Migration output is ready for Shopify import")
    else:
        print("⚠️  SOME CHECKS FAILED")
        print("⚠️  Please review the issues above before importing to Shopify")
    
    print()
    
    return all_checks_passed

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python verify_migration.py <output_csv_file>")
        sys.exit(1)
    
    output_file = sys.argv[1]
    if not Path(output_file).exists():
        print(f"❌ ERROR: Output file not found: {output_file}")
        sys.exit(1)
    
    success = verify_migration(output_file)
    sys.exit(0 if success else 1)

