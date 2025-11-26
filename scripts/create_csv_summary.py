import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.migration import normalize_product_name

def main():
    # Read the text report and create CSV summaries
    report_file = "data/output/missing_products_comprehensive_report.txt"
    
    if not Path(report_file).exists():
        print("Report file not found!")
        return
    
    # Parse the report to extract data
    with open(report_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Extract migrated products without description
    no_desc_section = content.split("MIGRATED PRODUCTS WITHOUT DESCRIPTION")[1].split("MIGRATED PRODUCTS WITHOUT PRICE")[0]
    no_desc_products = []
    for line in no_desc_section.split('\n'):
        if line.strip() and line.strip()[0].isdigit():
            product = line.split('. ', 1)[1] if '. ' in line else line.strip()
            if product and not product.startswith('Total'):
                no_desc_products.append({'Product Name': product, 'Issue': 'Missing Description'})
    
    # Extract missing products with all fields
    all_fields_section = content.split("MISSING PRODUCTS WITH ALL FIELDS")[1].split("MISSING PRODUCTS MISSING REQUIRED FIELDS")[0]
    all_fields_products = []
    for line in all_fields_section.split('\n'):
        if line.strip() and line.strip()[0].isdigit():
            product = line.split('. ', 1)[1] if '. ' in line else line.strip()
            if product and not product.startswith('These'):
                all_fields_products.append({'Product Name': product, 'Status': 'Has All Fields - Ready to Migrate'})
    
    # Extract complete missing list
    complete_section = content.split("COMPLETE LIST OF ALL MISSING PRODUCTS")[1]
    missing_products = []
    for line in complete_section.split('\n'):
        if line.strip() and line.strip()[0].isdigit():
            if '[' in line and ']' in line:
                product = line.split(' [')[0].split('. ', 1)[1] if '. ' in line else line.split(' [')[0]
                missing_fields = line.split('[Missing: ')[1].split(']')[0] if '[Missing: ' in line else ''
                missing_products.append({
                    'Product Name': product,
                    'Missing Fields': missing_fields,
                    'Can Migrate': 'Yes' if missing_fields == 'None (has all fields)' else 'No'
                })
    
    # Create DataFrames and save
    if no_desc_products:
        df_no_desc = pd.DataFrame(no_desc_products)
        df_no_desc.to_csv('data/output/migrated_products_without_description.csv', index=False)
        print(f"✅ Created: migrated_products_without_description.csv ({len(df_no_desc)} products)")
    
    if all_fields_products:
        df_all_fields = pd.DataFrame(all_fields_products)
        df_all_fields.to_csv('data/output/missing_products_with_all_fields.csv', index=False)
        print(f"✅ Created: missing_products_with_all_fields.csv ({len(df_all_fields)} products)")
    
    if missing_products:
        df_missing = pd.DataFrame(missing_products)
        df_missing.to_csv('data/output/all_missing_products.csv', index=False)
        print(f"✅ Created: all_missing_products.csv ({len(df_missing)} products)")
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Migrated products without description: {len(no_desc_products)}")
    print(f"Missing products with all fields (ready to migrate): {len(all_fields_products)}")
    print(f"Total missing products: {len(missing_products)}")
    print(f"  - Can migrate: {sum(1 for p in missing_products if p['Can Migrate'] == 'Yes')}")
    print(f"  - Cannot migrate (missing fields): {sum(1 for p in missing_products if p['Can Migrate'] == 'No')}")

if __name__ == "__main__":
    main()

