# Shopify Product Migration Plan

## 📋 Project Overview

This project migrates 1800+ products from a source CSV file to Shopify's product import format with 100% precision and data integrity.

**Goal**: Convert all product data from source CSV to Shopify-compatible CSV format without losing any information.

---

## 🏗️ Project Structure

```
ShopifyMigration/
├── data/
│   ├── source_products.csv          # Your original product data (1800+ products)
│   ├── shopify_template.csv         # Shopify format template/reference
│   ├── sample_10_products.csv       # First 10 products for testing
│   └── output/
│       ├── shopify_products_10.csv  # Converted 10 products (test output)
│       ├── shopify_products_all.csv # Final output (all 1800+ products)
│       └── error_report.csv         # Error log for failed rows
├── src/
│   ├── __init__.py
│   ├── mapper.py                    # Field mapping logic
│   ├── transformer.py               # Data transformation
│   ├── validator.py                 # Data validation
│   ├── csv_handler.py                # CSV read/write operations
│   └── migration.py                  # Main migration orchestrator
├── tests/
│   ├── __init__.py
│   ├── test_mapper.py
│   ├── test_transformer.py
│   ├── test_validator.py
│   ├── test_csv_handler.py
│   └── fixtures/
│       └── sample_data.csv
├── scripts/
│   ├── migrate_10_products.py       # Test with 10 products
│   └── migrate_all_products.py      # Full migration
├── config/
│   ├── field_mapping.json            # Field mapping configuration
│   └── config.yaml                  # General configuration
├── docs/
│   ├── MIGRATION_GUIDE.md
│   ├── FIELD_MAPPING.md
│   └── ERROR_HANDLING.md
├── logs/
│   └── migration_YYYY-MM-DD.log
├── requirements.txt
├── .env.example
├── .gitignore
├── README.md
└── pyproject.toml
```

---

## 📁 CSV File Location Configuration

### Method 1: Configuration File (Recommended)
Create a `config/config.yaml` file where you specify the paths:

```yaml
files:
  source_csv: "/path/to/your/source_products.csv"
  shopify_template: "/path/to/your/shopify_template.csv"
  output_dir: "./data/output"
  sample_size: 10  # For initial testing
```

### Method 2: Environment Variables
Create a `.env` file:

```env
SOURCE_CSV_PATH=/path/to/your/source_products.csv
SHOPIFY_TEMPLATE_PATH=/path/to/your/shopify_template.csv
OUTPUT_DIR=./data/output
```

### Method 3: Command Line Arguments
Pass paths directly when running scripts:

```bash
python scripts/migrate_10_products.py \
  --source /path/to/source_products.csv \
  --template /path/to/shopify_template.csv \
  --output ./data/output
```

**We'll implement all three methods for maximum flexibility!**

---

## 🎯 Migration Phases

### Phase 1: Setup and Analysis (Day 1)

#### Step 1.1: Environment Setup
- [ ] Create Python virtual environment
- [ ] Install all dependencies from `requirements.txt`
- [ ] Set up project structure
- [ ] Configure logging system

#### Step 1.2: CSV File Analysis
- [ ] Load source CSV and analyze structure
  - Column names
  - Data types
  - Sample rows (first 10)
  - Total row count
  - Missing values analysis
- [ ] Load Shopify template CSV
  - Required fields
  - Optional fields
  - Field formats and constraints
  - Data type requirements

#### Step 1.3: Field Mapping Documentation
- [ ] Create field mapping document
  - Source field → Shopify field mapping
  - Transformation rules
  - Default values
  - Special handling requirements
- [ ] Identify unmapped fields
- [ ] Document data transformations needed

#### Step 1.4: Configuration Setup
- [ ] Create `config/field_mapping.json` with mapping rules
- [ ] Create `config/config.yaml` with file paths and settings
- [ ] Set up `.env.example` template

---

### Phase 2: Core Development (Day 2-3)

#### Step 2.1: CSV Handler Module (`src/csv_handler.py`)
**Responsibilities:**
- Read CSV files with proper encoding handling
- Write CSV files in Shopify format
- Handle large files with chunking (for 1800+ products)
- Preserve data types
- Handle missing columns gracefully

**Features:**
- Auto-detect encoding (UTF-8, UTF-8-BOM, etc.)
- Progress tracking for large files
- Error handling for corrupted rows
- Support for different CSV delimiters

#### Step 2.2: Field Mapper Module (`src/mapper.py`)
**Responsibilities:**
- Load mapping configuration from JSON
- Apply field mapping rules
- Handle different mapping types:
  - Direct mapping (field A → field B)
  - Concatenation (field A + field B → field C)
  - Conditional mapping (if condition → field A else field B)
  - Default values
  - Data type conversion

**Mapping Types:**
```json
{
  "direct": {
    "source_field": "shopify_field"
  },
  "concatenate": {
    "target": "Title",
    "fields": ["Brand", "Model"],
    "separator": " "
  },
  "conditional": {
    "target": "Status",
    "condition": "if Inventory > 0 then 'active' else 'draft'"
  },
  "default": {
    "Type": "Product",
    "Vendor": "Default Vendor"
  }
}
```

#### Step 2.3: Data Transformer Module (`src/transformer.py`)
**Responsibilities:**
- Apply transformation rules to each row
- Handle Shopify-specific requirements:
  - Variant creation (if multiple options)
  - Image URL formatting
  - Tags formatting (comma-separated)
  - Price formatting (decimal places)
  - Inventory tracking
  - SEO fields (meta title, description)
  - Handle special characters
  - HTML tag cleaning (if needed)

**Transformations:**
- Price: Ensure 2 decimal places
- Images: Validate URLs, format as comma-separated
- Tags: Convert to comma-separated list
- Inventory: Ensure numeric, handle negative values
- Text fields: Clean special characters, trim whitespace

#### Step 2.4: Validator Module (`src/validator.py`)
**Responsibilities:**
- Pre-migration validation:
  - Required fields check
  - Data type validation
  - Value range checks
  - Format validation (URLs, emails, etc.)
  - Duplicate detection (SKUs, handles)
- Post-migration validation:
  - Compare row counts
  - Verify critical fields populated
  - Check for data loss
  - Validate Shopify format compliance

**Validation Rules:**
- Required fields must not be empty
- SKUs must be unique
- Prices must be positive numbers
- Image URLs must be valid format
- Inventory must be integer >= 0
- Handles must be URL-friendly (no spaces, special chars)

#### Step 2.5: Migration Orchestrator (`src/migration.py`)
**Responsibilities:**
- Coordinate all modules
- Handle error collection and reporting
- Generate progress reports
- Create error logs
- Generate summary statistics

**Flow:**
1. Load configuration
2. Read source CSV
3. For each row:
   - Validate source data
   - Map fields
   - Transform data
   - Validate transformed data
   - Write to output CSV
   - Log errors if any
4. Generate summary report

---

### Phase 3: Testing (Day 4)

#### Step 3.1: Unit Tests
- [ ] Test CSV handler with various file formats
- [ ] Test mapper with different mapping rules
- [ ] Test transformer with edge cases
- [ ] Test validator with invalid data
- [ ] Achieve 80%+ code coverage

#### Step 3.2: Integration Test - 10 Products
- [ ] Extract first 10 products from source CSV
- [ ] Run migration script
- [ ] Verify output CSV structure matches Shopify template
- [ ] Manually verify each field for all 10 products
- [ ] Check error logs
- [ ] Verify no data loss

#### Step 3.3: Validation Checks
- [ ] All required Shopify fields present
- [ ] Data types match Shopify requirements
- [ ] No null values in required fields
- [ ] Format compliance (prices, dates, etc.)
- [ ] Image URLs valid
- [ ] SKUs unique
- [ ] Handles properly formatted

#### Step 3.4: Edge Case Testing
- [ ] Missing optional fields
- [ ] Special characters in text fields
- [ ] Very long text fields
- [ ] Empty rows
- [ ] Duplicate SKUs in source
- [ ] Invalid data types
- [ ] Missing required fields

---

### Phase 4: Full Migration & Documentation (Day 5)

#### Step 4.1: Full Migration Execution
- [ ] Run migration for all 1800+ products
- [ ] Monitor progress and errors
- [ ] Generate final output CSV
- [ ] Create error report
- [ ] Generate summary statistics

#### Step 4.2: Quality Assurance
- [ ] Verify row count (accounting for variants)
- [ ] Spot-check random products
- [ ] Verify critical fields
- [ ] Check error rate
- [ ] Validate file format

#### Step 4.3: Documentation
- [ ] Create `docs/MIGRATION_GUIDE.md` with step-by-step instructions
- [ ] Create `docs/FIELD_MAPPING.md` with complete field mapping reference
- [ ] Create `docs/ERROR_HANDLING.md` with error resolution guide
- [ ] Update `README.md` with project overview and quick start

#### Step 4.4: Final Review
- [ ] Review all migrated products
- [ ] Fix any critical errors
- [ ] Prepare final CSV for Shopify import
- [ ] Create migration summary report

---

## 🛠️ Technical Implementation Details

### Python Packages Required

```txt
# Core dependencies
pandas>=2.0.0          # CSV handling and data manipulation
numpy>=1.24.0          # Numerical operations
python-dotenv>=1.0.0   # Environment variables
pyyaml>=6.0            # YAML configuration files

# Validation
pydantic>=2.0.0        # Data validation with type hints
cerberus>=1.3.4        # Schema validation

# Testing
pytest>=7.4.0          # Testing framework
pytest-cov>=4.1.0      # Coverage reports
pytest-mock>=3.11.0    # Mocking utilities

# Utilities
tqdm>=4.66.0           # Progress bars
colorama>=0.4.6        # Colored terminal output
loguru>=0.7.0          # Advanced logging

# Documentation (optional)
sphinx>=7.0.0          # Documentation generation
```

### Error Handling Strategy

#### Logging Levels
- **INFO**: Progress updates, successful operations
- **WARNING**: Data issues (non-critical), missing optional fields
- **ERROR**: Critical failures, missing required fields
- **DEBUG**: Detailed debugging information

#### Error Types & Handling

1. **Missing Required Fields**
   - Action: Skip row, log error with row number
   - Report: Add to error report CSV
   - Continue: Yes (skip row only)

2. **Invalid Data Format**
   - Action: Attempt automatic fix, log warning
   - Report: Add to warning log
   - Continue: Yes (use fixed/default value)

3. **Missing Mapping**
   - Action: Use default value if configured, else skip field
   - Report: Log warning
   - Continue: Yes

4. **File Errors**
   - Action: Fail fast with clear error message
   - Report: Display error immediately
   - Continue: No (stop migration)

5. **Data Type Mismatch**
   - Action: Attempt conversion, log if fails
   - Report: Add to warning log
   - Continue: Yes (use default or skip)

#### Error Report Format
```csv
row_number,field_name,error_type,error_message,original_value,suggested_fix
5,Price,invalid_format,"Price must be numeric","$19.99","19.99"
12,SKU,duplicate,"SKU already exists","ABC123","ABC123-2"
```

---

## ✅ Data Validation Checklist

### Pre-Migration Validation
- [ ] Source CSV file exists and is readable
- [ ] Shopify template CSV exists and is readable
- [ ] All required Shopify fields have mappings
- [ ] Field mapping configuration is valid
- [ ] Sample data loads correctly

### Post-Migration Validation
- [ ] Output CSV file created successfully
- [ ] Row count matches (or accounts for variants)
- [ ] All required fields populated
- [ ] No null values in required fields
- [ ] Data types match Shopify requirements
- [ ] Price formats correct (2 decimal places)
- [ ] Image URLs valid format
- [ ] SKUs unique
- [ ] Inventory numbers valid (>= 0)
- [ ] Tags properly formatted (comma-separated)
- [ ] SEO fields populated
- [ ] Handles URL-friendly
- [ ] No data loss (all source data represented)

---

## 📊 Success Metrics

1. **Data Completeness**: 100% of source products migrated
2. **Data Accuracy**: All fields correctly mapped and transformed
3. **Format Compliance**: 100% Shopify format compliance
4. **Error Rate**: < 1% of rows with errors
5. **Performance**: Complete migration in < 30 minutes

---

## 🚀 Usage Instructions

### Initial Setup
```bash
# 1. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure file paths
# Edit config/config.yaml or create .env file
```

### Test Migration (10 Products)
```bash
# Method 1: Using config file
python scripts/migrate_10_products.py

# Method 2: Using command line arguments
python scripts/migrate_10_products.py \
  --source /path/to/source_products.csv \
  --template /path/to/shopify_template.csv \
  --output ./data/output

# Method 3: Using environment variables
export SOURCE_CSV_PATH=/path/to/source_products.csv
export SHOPIFY_TEMPLATE_PATH=/path/to/shopify_template.csv
python scripts/migrate_10_products.py
```

### Full Migration (All Products)
```bash
python scripts/migrate_all_products.py
```

### Run Tests
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_mapper.py
```

---

## 📝 Next Steps

1. **Provide CSV Files**:
   - Place `source_products.csv` in `data/` directory
   - Place `shopify_template.csv` in `data/` directory
   - Or update `config/config.yaml` with file paths

2. **Review Field Mapping**:
   - Analyze source CSV columns
   - Compare with Shopify template
   - Create initial mapping configuration

3. **Start Implementation**:
   - Set up project structure
   - Implement core modules
   - Test with 10 products
   - Iterate based on results

---

## 🔍 Quality Assurance Process

1. **Code Review**: All code reviewed before merging
2. **Testing**: Comprehensive unit and integration tests
3. **Validation**: Multiple validation checkpoints
4. **Documentation**: Complete documentation for all modules
5. **Error Handling**: Robust error handling and reporting
6. **Logging**: Detailed logging for debugging and audit trail

---

## 📞 Support & Troubleshooting

- Check `logs/migration_YYYY-MM-DD.log` for detailed error messages
- Review `data/output/error_report.csv` for row-level errors
- Consult `docs/ERROR_HANDLING.md` for common issues and solutions
- Verify field mapping in `config/field_mapping.json`

---

**Last Updated**: [Date]
**Version**: 1.0.0
**Status**: Planning Phase
```

## CSV file location approach

Three ways to specify CSV file locations:

### Method 1: Configuration file (recommended)
Create `config/config.yaml`:
```yaml
files:
  source_csv: "/home/yuvraj/Downloads/your_products.csv"
  shopify_template: "/home/yuvraj/Downloads/shopify_template.csv"
  output_dir: "./data/output"
```

### Method 2: Environment variables
Create `.env`:
```env
SOURCE_CSV_PATH=/home/yuvraj/Downloads/your_products.csv
SHOPIFY_TEMPLATE_PATH=/home/yuvraj/Downloads/shopify_template.csv
```

### Method 3: Command line arguments
```bash
python scripts/migrate_10_products.py \
  --source /home/yuvraj/Downloads/your_products.csv \
  --template /home/yuvraj/Downloads/shopify_template.csv
```

The script will:
1. Check command line arguments first
2. Fall back to environment variables
3. Fall back to config file
4. Use default paths if nothing is specified

Since you're in ask mode, create the `MIGRATION_PLAN.md` file with the content above. After you switch to agent mode, I can:
1. Create the project structure
2. Implement the modules
3. Set up the configuration system

Should I proceed with creating the project structure and initial files?
