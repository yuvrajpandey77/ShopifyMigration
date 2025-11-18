# Error Handling Guide

Guide to understanding and resolving migration errors.

## Error Types

### 1. Source Validation Errors

Errors that occur when validating source data before transformation.

**Common Causes:**
- Empty rows
- Missing critical fields
- Invalid data types

**Resolution:**
- Remove or fix empty rows in source CSV
- Ensure all required source fields are present
- Fix data type issues in source data

### 2. Mapping Errors

Errors that occur during field mapping.

**Common Causes:**
- Missing field in mapping configuration
- Incorrect field names
- Missing default values

**Resolution:**
- Update `config/field_mapping.json`
- Add missing field mappings
- Add default values for optional fields

### 3. Transformation Errors

Errors that occur during data transformation.

**Common Causes:**
- Invalid price format
- Invalid inventory value
- Invalid URL format
- Invalid handle format

**Resolution:**
- Check transformer rules
- Fix source data formats
- Review validation warnings

### 4. Validation Errors

Errors that occur when validating transformed data.

**Common Causes:**
- Missing required Shopify fields
- Invalid data formats
- Duplicate SKUs
- Invalid field values

**Resolution:**
- Complete field mapping
- Fix data formats
- Resolve duplicate SKUs
- Review Shopify requirements

## Error Report

After migration, check `data/output/error_report_*.csv`:

```csv
row_number,error_type,errors,warnings
5,validation,"Missing required field 'Title'",
12,transformation,"Invalid price format: $19.99",
```

### Understanding Error Report

- **row_number**: Row number in source CSV (including header)
- **error_type**: Type of error (source_validation, mapping, transformation, validation)
- **errors**: List of errors for this row
- **warnings**: List of warnings for this row

## Common Errors and Solutions

### Error: Missing Required Field

**Message:** `Missing required field 'Title'`

**Solution:**
1. Check field mapping configuration
2. Ensure source field is mapped to required Shopify field
3. Add default value if source field doesn't exist

### Error: Invalid Price Format

**Message:** `Invalid price format in 'Variant Price': $19.99`

**Solution:**
1. Transformer should handle this automatically
2. If not, check source data format
3. Update transformer if needed

### Error: Duplicate SKU

**Message:** `Duplicate SKU found: SKU001`

**Solution:**
1. Review source data for duplicate SKUs
2. Make SKUs unique (add suffix, etc.)
3. Or handle variants properly

### Error: Invalid Handle

**Message:** `Invalid handle format in 'Handle': My Product!`

**Solution:**
1. Transformer should convert to URL-friendly format
2. Check if transformation is working
3. Manually fix if needed

### Error: File Not Found

**Message:** `CSV file not found: /path/to/file.csv`

**Solution:**
1. Check file path in configuration
2. Verify file exists
3. Check file permissions

### Error: Encoding Error

**Message:** `UnicodeDecodeError`

**Solution:**
1. Tool auto-detects encoding
2. If issues persist, check source file encoding
3. Convert source file to UTF-8

## Warning Types

Warnings don't stop migration but indicate potential issues:

### Missing Optional Field

**Warning:** `Missing optional field 'SEO Title'`

**Impact:** Field will be empty in output
**Action:** Add mapping or default value if needed

### Invalid Image URL

**Warning:** `Invalid image URL: not-a-url`

**Impact:** Image won't be imported
**Action:** Fix URL in source data

### Boolean Format Warning

**Warning:** `Boolean field 'Published' should be TRUE/FALSE, got: yes`

**Impact:** May cause import issues
**Action:** Transformer should fix this automatically

## Error Resolution Workflow

1. **Review Error Report**
   - Open `data/output/error_report_*.csv`
   - Identify error types
   - Count errors by type

2. **Fix Source Data** (if needed)
   - Fix data quality issues
   - Remove invalid rows
   - Fix formats

3. **Update Configuration** (if needed)
   - Update field mapping
   - Add missing mappings
   - Add default values

4. **Re-run Migration**
   - Test with 10 products first
   - Verify errors are resolved
   - Run full migration

5. **Validate Output**
   - Check error count
   - Review sample products
   - Verify data quality

## Best Practices

1. **Fix Source Data First**: Address data quality issues at source
2. **Complete Mapping**: Ensure all required fields are mapped
3. **Use Defaults**: Set defaults for common fields
4. **Test Incrementally**: Test after each fix
5. **Document Issues**: Keep notes of resolved issues

## Debugging Tips

### Enable Debug Logging

Edit `config/config.yaml`:

```yaml
logging:
  level: "DEBUG"
```

### Check Logs

Review `logs/migration_*.log` for detailed information:

```bash
tail -f logs/migration_*.log
```

### Test Individual Components

```python
from src.mapper import FieldMapper
from src.transformer import DataTransformer

# Test mapping
mapper = FieldMapper('config/field_mapping.json')
result = mapper.map_row(source_row)

# Test transformation
transformer = DataTransformer()
result = transformer.transform_row(mapped_row)
```

## Getting Help

If errors persist:

1. Check logs in `logs/` directory
2. Review error report in `data/output/`
3. Verify field mapping configuration
4. Test with sample data
5. Check Shopify import requirements

## Error Prevention

1. **Validate Source Data**: Check data quality before migration
2. **Complete Mapping**: Map all required fields
3. **Test First**: Always test with sample data
4. **Review Template**: Understand Shopify requirements
5. **Document Changes**: Keep track of configuration changes

---

For migration steps, see `MIGRATION_GUIDE.md`.  
For field mapping, see `FIELD_MAPPING.md`.

