# Shopify Product Migration Tool

A comprehensive Python tool for migrating products from CSV format to Shopify's product import format with 100% precision and data integrity.

## 🎯 Features

- **Complete Field Mapping**: Flexible configuration-based field mapping system
- **Data Transformation**: Automatic transformation to Shopify format requirements
- **Validation**: Comprehensive pre and post-migration validation
- **Error Handling**: Detailed error reporting and logging
- **Testing Support**: Built-in support for testing with sample products
- **Progress Tracking**: Real-time progress bars and logging
- **Multiple Configuration Methods**: Support for config files, environment variables, and CLI arguments

## 📋 Requirements

- Python 3.9 or higher
- See `requirements.txt` for package dependencies

## 🚀 Quick Start

### 1. Installation

```bash
# Clone or navigate to the project directory
cd ShopifyMigration

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

You have three options to configure file paths:

#### Option A: Configuration File (Recommended)

Edit `config/config.yaml`:

```yaml
files:
  source_csv: "/path/to/your/source_products.csv"
  shopify_template: "/path/to/your/shopify_template.csv"
  output_dir: "./data/output"
```

#### Option B: Environment Variables

Create a `.env` file:

```env
SOURCE_CSV_PATH=/path/to/your/source_products.csv
SHOPIFY_TEMPLATE_PATH=/path/to/your/shopify_template.csv
OUTPUT_DIR=./data/output
```

#### Option C: Command Line Arguments

Pass paths directly when running scripts (see Usage section).

### 3. Field Mapping Configuration

Edit `config/field_mapping.json` to define how source fields map to Shopify fields. See `docs/FIELD_MAPPING.md` for detailed instructions.

### 4. Test Migration (10 Products)

Before migrating all products, test with a small sample:

```bash
python scripts/migrate_10_products.py
```

Or with custom paths:

```bash
python scripts/migrate_10_products.py \
  --source /path/to/source_products.csv \
  --template /path/to/shopify_template.csv \
  --output ./data/output
```

### 5. Full Migration

Once you've verified the test migration:

```bash
python scripts/migrate_all_products.py
```

Or with custom paths:

```bash
python scripts/migrate_all_products.py \
  --source /path/to/source_products.csv \
  --template /path/to/shopify_template.csv \
  --output ./data/output
```

## 📁 Project Structure

```
ShopifyMigration/
├── data/                    # CSV files (source, template, output)
├── src/                     # Core modules
│   ├── csv_handler.py      # CSV read/write operations
│   ├── mapper.py           # Field mapping logic
│   ├── transformer.py      # Data transformation
│   ├── validator.py        # Data validation
│   └── migration.py        # Migration orchestrator
├── scripts/                 # Migration scripts
│   ├── migrate_10_products.py
│   └── migrate_all_products.py
├── config/                  # Configuration files
│   ├── config.yaml
│   └── field_mapping.json
├── tests/                   # Test suite
├── docs/                    # Documentation
└── logs/                    # Log files
```

## 🔧 Usage

### Command Line Options

Both migration scripts support the following options:

- `--source`: Path to source products CSV file
- `--template`: Path to Shopify template CSV file
- `--output`: Output directory for migrated CSV
- `--mapping`: Path to field mapping configuration JSON
- `--config`: Path to configuration YAML file (default: `config/config.yaml`)
- `--sample-size`: Number of products for testing (migrate_10_products.py only)
- `--confirm`: Skip confirmation prompt (migrate_all_products.py only)

### Examples

```bash
# Test with 10 products using config file
python scripts/migrate_10_products.py

# Test with 20 products using command line
python scripts/migrate_10_products.py --sample-size 20

# Full migration with custom paths
python scripts/migrate_all_products.py \
  --source ./data/products.csv \
  --template ./data/shopify_template.csv \
  --output ./data/output

# Full migration with confirmation skipped
python scripts/migrate_all_products.py --confirm
```

## 📊 Output Files

After migration, you'll find:

- `shopify_products_10.csv` or `shopify_products_all.csv`: The migrated products in Shopify format
- `error_report_YYYYMMDD_HHMMSS.csv`: Detailed error report (if any errors occurred)
- `logs/migration_YYYYMMDD_HHMMSS.log`: Detailed migration log

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_mapper.py
```

## 📚 Documentation

- `MIGRATION_PLAN.md`: Complete migration plan and phases
- `docs/MIGRATION_GUIDE.md`: Step-by-step migration guide
- `docs/FIELD_MAPPING.md`: Field mapping configuration guide
- `docs/ERROR_HANDLING.md`: Error handling and troubleshooting

## ⚠️ Important Notes

1. **Always test first**: Run `migrate_10_products.py` before full migration
2. **Backup your data**: Keep backups of your source CSV files
3. **Review field mapping**: Ensure `config/field_mapping.json` is correctly configured
4. **Check error reports**: Review error reports before importing to Shopify
5. **Validate output**: Manually verify a few products in the output CSV

## 🐛 Troubleshooting

### Common Issues

1. **File not found errors**: Check file paths in config or command line arguments
2. **Encoding errors**: The tool auto-detects encoding, but you can specify it in the code if needed
3. **Missing required fields**: Update `field_mapping.json` to map all required Shopify fields
4. **Invalid data format**: Check the transformer module for format requirements

See `docs/ERROR_HANDLING.md` for detailed troubleshooting guide.

## 📝 License

This project is provided as-is for product migration purposes.

## 🤝 Contributing

This is a migration tool. For improvements or bug fixes, please document changes and test thoroughly.

## 📞 Support

For issues or questions:
1. Check the logs in `logs/` directory
2. Review error reports in `data/output/`
3. Consult documentation in `docs/` directory

---

**Version**: 1.0.0  
**Last Updated**: 2024

