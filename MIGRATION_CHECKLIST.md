# Complete Migration Verification Checklist

## Overview
This document lists all critical checks performed on the Shopify migration output to ensure data quality and import readiness.

## Migration Statistics
- **Total Source Products**: 56,815
- **Output File**: `data/output/shopify_products_FULL_MIGRATION.csv`

---

## 1. PRICE VERIFICATION ✅

### Checks:
- [ ] All variant rows have valid prices (> $0.00)
- [ ] All single products (without variants) have valid prices
- [ ] Parent rows with variants have empty Variant Price (correct)
- [ ] No zero prices in any product/variant
- [ ] Price format is correct (numeric, no currency symbols in value)

### Expected Results:
- ✅ All variants: Have prices > 0
- ✅ All single products: Have prices > 0
- ✅ Parent rows with variants: Variant Price is empty

---

## 2. DESCRIPTION VERIFICATION ✅

### Checks:
- [ ] All parent products have descriptions
- [ ] Descriptions include tab styling (Overview, Specifications, Additional)
- [ ] Descriptions have responsive CSS design
- [ ] Descriptions have JavaScript functionality for tab switching
- [ ] Descriptions are properly formatted HTML

### Expected Results:
- ✅ All parent products: Have Body (HTML) field populated
- ✅ Tab styling: Present in all descriptions
- ✅ Responsive design: @media queries present
- ✅ JavaScript: Tab switching functionality included

---

## 3. IMAGE VERIFICATION ✅

### Checks:
- [ ] All parent products have at least one image
- [ ] Image URLs are valid and accessible
- [ ] Products with multiple images have additional image rows
- [ ] Variant images are correctly assigned
- [ ] Image Src field is properly formatted

### Expected Results:
- ✅ All parent products: Have Image Src populated
- ✅ Multiple images: Additional rows created for products with multiple images
- ✅ Image URLs: Valid format and accessible

---

## 4. VARIANT VERIFICATION ✅

### Checks:
- [ ] Variant rows have empty Title field (correct for Shopify)
- [ ] Variant rows have Option1 Value populated
- [ ] Parent rows with variants have Option1 Name set (e.g., "Size")
- [ ] Parent rows with variants have empty Option1 Value
- [ ] Single products have empty Option1 Name and Option1 Value
- [ ] Variants are correctly grouped under parent products
- [ ] All variants share the same Handle as parent

### Expected Results:
- ✅ Variant rows: Title is empty
- ✅ Variant rows: Option1 Value is populated
- ✅ Parent with variants: Option1 Name = "Size", Option1 Value = empty
- ✅ Single products: Option1 Name = empty, Option1 Value = empty

---

## 5. INVENTORY VERIFICATION ✅

### Checks:
- [ ] All variants have Variant Inventory Tracker set to "shopify"
- [ ] All single products have Variant Inventory Tracker set to "shopify"
- [ ] Variants with inventory > 0 have tracker set to "shopify"
- [ ] Inventory quantities are positive integers
- [ ] No "inventory not tracked" errors

### Expected Results:
- ✅ All variants: Variant Inventory Tracker = "shopify"
- ✅ All single products: Variant Inventory Tracker = "shopify"
- ✅ Inventory quantities: Positive integers where applicable

---

## 6. CATEGORY VERIFICATION ✅

### Checks:
- [ ] All parent products have Product category populated
- [ ] Categories use valid Shopify taxonomy (top-level "Sporting Goods")
- [ ] No "uncategorised" or "Uncategorised" text in categories
- [ ] No empty categories
- [ ] Categories are properly formatted

### Expected Results:
- ✅ All parent products: Have Product category
- ✅ Category format: "Sporting Goods" (valid Shopify taxonomy)
- ✅ No uncategorised products

---

## 7. FULFILLMENT SERVICE VERIFICATION ✅

### Checks:
- [ ] All variants have Variant Fulfillment Service set to "manual"
- [ ] All single products have Variant Fulfillment Service set to "manual"
- [ ] Parent rows with variants have empty Variant Fulfillment Service (correct)
- [ ] No blank fulfillment service fields

### Expected Results:
- ✅ All variants: Variant Fulfillment Service = "manual"
- ✅ All single products: Variant Fulfillment Service = "manual"
- ✅ Parent with variants: Variant Fulfillment Service = empty

---

## 8. INVENTORY POLICY VERIFICATION ✅

### Checks:
- [ ] All variants have Variant Inventory Policy set to "deny" or "continue"
- [ ] All single products have Variant Inventory Policy set to "deny" or "continue"
- [ ] No invalid policy values
- [ ] Policy values are lowercase

### Expected Results:
- ✅ All variants: Variant Inventory Policy = "deny" or "continue"
- ✅ All single products: Variant Inventory Policy = "deny" or "continue"
- ✅ No invalid values

---

## 9. SKU VERIFICATION ✅

### Checks:
- [ ] All variants have Variant SKU populated
- [ ] All single products have Variant SKU populated
- [ ] No duplicate SKUs
- [ ] SKU format is valid
- [ ] Parent rows with variants have empty Variant SKU (correct)

### Expected Results:
- ✅ All variants: Have Variant SKU
- ✅ All single products: Have Variant SKU
- ✅ No duplicate SKUs
- ✅ Parent with variants: Variant SKU = empty

---

## 10. HANDLE VERIFICATION ✅

### Checks:
- [ ] All parent products have Handle populated
- [ ] All variant rows have Handle populated
- [ ] Variants share the same Handle as their parent
- [ ] Handles are URL-friendly (no special characters)
- [ ] No duplicate handles for different products

### Expected Results:
- ✅ All parent products: Have Handle
- ✅ All variant rows: Have Handle matching parent
- ✅ Handle format: URL-friendly, lowercase, hyphens

---

## 11. ADDITIONAL CHECKS ✅

### Required Fields:
- [ ] Title: All parent products have Title
- [ ] Vendor: All products have Vendor (default if not in source)
- [ ] Type: Properly set or empty
- [ ] Status: "active" or "draft"
- [ ] Published: "TRUE" or "FALSE"

### Data Quality:
- [ ] No NaN values in critical fields
- [ ] No empty strings where values are required
- [ ] Proper data types (strings, numbers)
- [ ] No encoding issues (UTF-8)

---

## Verification Script

Run the comprehensive verification script:

```bash
./venv/bin/python scripts/verify_migration.py data/output/shopify_products_FULL_MIGRATION.csv
```

This script performs all checks automatically and provides a detailed report.

---

## Final Checklist Before Import

Before importing to Shopify, ensure:

- [ ] All price checks pass
- [ ] All description checks pass
- [ ] All image checks pass
- [ ] All variant checks pass
- [ ] All inventory checks pass
- [ ] All category checks pass
- [ ] All fulfillment service checks pass
- [ ] All inventory policy checks pass
- [ ] All SKU checks pass
- [ ] All handle checks pass
- [ ] Verification script reports: ✅ ALL CHECKS PASSED

---

## Notes

- The migration processes all 56,815 products from the source CSV
- Products are automatically grouped into variants based on base name
- Categories are mapped to Shopify's "Sporting Goods" taxonomy
- Descriptions include interactive tab styling with responsive design
- All critical fields are validated and corrected automatically

---

**Last Updated**: Migration completion time
**Status**: Ready for verification

