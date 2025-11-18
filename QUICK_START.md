# Quick Start Guide

Get started with Shopify product migration in 5 minutes!

## Step 1: Install Dependencies

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install packages
pip install -r requirements.txt
```

## Step 2: Prepare Your Files

1. Place your source products CSV file somewhere accessible
2. Get Shopify template CSV (download from Shopify admin: Products > Import)
3. Note the file paths

## Step 3: Configure File Paths

Edit `config/config.yaml`:

```yaml
files:
  source_csv: "/path/to/your/products.csv"
  shopify_template: "/path/to/shopify_template.csv"
  output_dir: "./data/output"
```

Or use environment variables (create `.env` file):

```env
SOURCE_CSV_PATH=/path/to/your/products.csv
SHOPIFY_TEMPLATE_PATH=/path/to/shopify_template.csv
OUTPUT_DIR=./data/output
```

## Step 4: Analyze Your Source CSV (Optional but Recommended)

```bash
python scripts/analyze_csv.py /path/to/your/products.csv
```

This will show you:
- Column names
- Data types
- Missing values
- Sample rows

## Step 5: Configure Field Mapping

Edit `config/field_mapping.json` to map your source fields to Shopify fields.

**Example:**
```json
{
  "mappings": {
    "direct": {
      "fields": {
        "product_name": "Title",
        "price": "Variant Price",
        "sku": "Variant SKU"
      }
    },
    "default": {
      "fields": {
        "Type": "Product",
        "Published": "TRUE"
      }
    }
  }
}
```

See `docs/FIELD_MAPPING.md` for detailed mapping instructions.

## Step 6: Test with 10 Products

```bash
python scripts/migrate_10_products.py
```

Review the output:
- Check `data/output/shopify_products_10.csv`
- Verify fields are correctly mapped
- Check for any errors

## Step 7: Full Migration

Once you're satisfied with the test:

```bash
python scripts/migrate_all_products.py
```

## Step 8: Import to Shopify

1. Download `data/output/shopify_products_all.csv`
2. Go to Shopify Admin > Products > Import
3. Upload the CSV file
4. Review and complete import

## Troubleshooting

- **File not found?** Check paths in `config/config.yaml` or command line arguments
- **Missing fields?** Update `config/field_mapping.json`
- **Errors?** Check `data/output/error_report_*.csv` and `logs/` directory

## Need Help?

- **Field Mapping**: See `docs/FIELD_MAPPING.md`
- **Migration Steps**: See `docs/MIGRATION_GUIDE.md`
- **Errors**: See `docs/ERROR_HANDLING.md`
- **Full Plan**: See `MIGRATION_PLAN.md`

---

**That's it!** You're ready to migrate your products to Shopify! 🚀

