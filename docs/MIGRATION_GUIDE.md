# Migration Guide

Step-by-step guide for migrating products to Shopify format.

## Prerequisites

1. Python 3.9+ installed
2. Source products CSV file
3. Shopify template CSV file (download from Shopify admin)
4. Understanding of your source data structure

## Step 1: Setup

### Install Dependencies

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Prepare Your Files

1. Place your source products CSV in `data/` directory or note its path
2. Place Shopify template CSV in `data/` directory or note its path
3. Ensure both files are accessible

## Step 2: Analyze Your Data

### Analyze Source CSV

You can use the CSV handler to analyze your source file:

```python
from src.csv_handler import CSVHandler

handler = CSVHandler()
analysis = handler.analyze_csv('data/source_products.csv')
print(analysis)
```

This will show you:
- Total row count
- Column names
- Data types
- Missing values
- Sample rows

### Review Shopify Template

Open the Shopify template CSV to understand:
- Required fields
- Field formats
- Data type requirements

## Step 3: Configure Field Mapping

Edit `config/field_mapping.json` to map your source fields to Shopify fields.

### Direct Mapping

```json
{
  "mappings": {
    "direct": {
      "fields": {
        "product_name": "Title",
        "price": "Variant Price",
        "sku": "Variant SKU"
      }
    }
  }
}
```

### Concatenation

```json
{
  "mappings": {
    "concatenate": {
      "fields": {
        "title": {
          "target": "Title",
          "fields": ["brand", "model"],
          "separator": " "
        }
      }
    }
  }
}
```

### Conditional Mapping

```json
{
  "mappings": {
    "conditional": {
      "fields": {
        "status": {
          "target": "Status",
          "condition": {
            "field": "inventory",
            "operator": ">",
            "value": 0,
            "then": "active",
            "else": "draft"
          }
        }
      }
    }
  }
}
```

### Default Values

```json
{
  "mappings": {
    "default": {
      "fields": {
        "Type": "Product",
        "Published": "TRUE",
        "Vendor": "Your Store"
      }
    }
  }
}
```

## Step 4: Test Migration

### Run Test Migration

```bash
python scripts/migrate_10_products.py
```

### Review Output

1. Check `data/output/shopify_products_10.csv`
2. Verify all fields are correctly mapped
3. Check for any errors in the error report
4. Manually verify a few products

### Common Issues in Test

- **Missing fields**: Add to field mapping
- **Wrong data types**: Check transformer rules
- **Invalid formats**: Review validation errors

## Step 5: Full Migration

### Update Configuration

Ensure `config/config.yaml` has correct paths:

```yaml
files:
  source_csv: "/path/to/your/source_products.csv"
  shopify_template: "/path/to/your/shopify_template.csv"
  output_dir: "./data/output"
```

### Run Full Migration

```bash
python scripts/migrate_all_products.py
```

Or skip confirmation:

```bash
python scripts/migrate_all_products.py --confirm
```

### Monitor Progress

The script will show:
- Progress bar
- Real-time statistics
- Error count
- Warnings

## Step 6: Validate Output

### Check Statistics

After migration, review:
- Total rows processed
- Successfully migrated count
- Failed rows count
- Error rate

### Review Error Report

If errors occurred:
1. Open `data/output/error_report_*.csv`
2. Review error types
3. Fix source data if needed
4. Re-run migration for failed rows

### Validate Data Quality

1. Check for duplicate SKUs
2. Verify required fields are populated
3. Validate price formats
4. Check image URLs
5. Verify handles are URL-friendly

## Step 7: Import to Shopify

1. Download the output CSV file
2. Log in to Shopify Admin
3. Go to Products > Import
4. Upload the CSV file
5. Review the import preview
6. Complete the import

## Troubleshooting

### High Error Rate

- Review error report
- Check field mapping configuration
- Verify source data quality
- Check for missing required fields

### Data Loss

- Compare row counts
- Check for filtered rows
- Review validation rules
- Verify mapping completeness

### Format Issues

- Check price formats (2 decimal places)
- Verify boolean fields (TRUE/FALSE)
- Validate image URLs
- Check handle format

## Best Practices

1. **Always test first**: Use 10-product test before full migration
2. **Backup data**: Keep copies of source and output files
3. **Document mapping**: Document any custom transformations
4. **Review errors**: Don't ignore error reports
5. **Validate output**: Manually check sample products
6. **Test import**: Import test file to Shopify first

## Next Steps

After successful migration:
1. Import to Shopify
2. Verify products in Shopify admin
3. Check product pages
4. Test checkout process
5. Monitor for any issues

---

For detailed field mapping instructions, see `FIELD_MAPPING.md`.  
For error handling, see `ERROR_HANDLING.md`.

