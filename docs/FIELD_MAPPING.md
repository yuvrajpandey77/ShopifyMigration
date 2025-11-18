# Field Mapping Guide

Complete guide to configuring field mappings for Shopify product migration.

## Overview

Field mapping defines how source CSV fields are transformed into Shopify product fields. This is configured in `config/field_mapping.json`.

## Mapping Types

### 1. Direct Mapping

Maps a source field directly to a Shopify field.

**Example:**
```json
{
  "mappings": {
    "direct": {
      "fields": {
        "product_name": "Title",
        "price": "Variant Price",
        "sku": "Variant SKU",
        "description": "Body (HTML)"
      }
    }
  }
}
```

**Source CSV:**
```csv
product_name,price,sku,description
"Product 1",19.99,SKU001,"Great product"
```

**Result:**
- `Title` = "Product 1"
- `Variant Price` = "19.99"
- `Variant SKU` = "SKU001"
- `Body (HTML)` = "Great product"

### 2. Concatenation Mapping

Combines multiple source fields into one Shopify field.

**Example:**
```json
{
  "mappings": {
    "concatenate": {
      "fields": {
        "title": {
          "target": "Title",
          "fields": ["brand", "model", "color"],
          "separator": " "
        }
      }
    }
  }
}
```

**Source CSV:**
```csv
brand,model,color
Apple,iPhone 13,Blue
```

**Result:**
- `Title` = "Apple iPhone 13 Blue"

### 3. Conditional Mapping

Maps fields based on conditions.

**Example:**
```json
{
  "mappings": {
    "conditional": {
      "fields": {
        "status": {
          "target": "Status",
          "condition": {
            "field": "inventory_qty",
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

**Operators:**
- `>`: Greater than
- `<`: Less than
- `>=`: Greater than or equal
- `<=`: Less than or equal
- `==`: Equal to
- `!=`: Not equal to
- `contains`: Contains substring
- `empty`: Field is empty

### 4. Default Values

Sets default values for fields not in source data.

**Example:**
```json
{
  "mappings": {
    "default": {
      "fields": {
        "Type": "Product",
        "Published": "TRUE",
        "Vendor": "My Store",
        "Variant Inventory Tracker": "shopify"
      }
    }
  }
}
```

## Shopify Required Fields

These fields are typically required by Shopify:

- `Handle`: URL-friendly product identifier
- `Title`: Product title
- `Body (HTML)`: Product description
- `Vendor`: Product vendor/brand
- `Type`: Product type
- `Tags`: Comma-separated tags
- `Published`: TRUE or FALSE
- `Option1 Name`: First variant option name (e.g., "Size")
- `Option1 Value`: First variant option value (e.g., "Large")
- `Variant SKU`: Product SKU
- `Variant Price`: Product price
- `Variant Inventory Tracker`: "shopify" or "not tracked"
- `Variant Inventory Qty`: Inventory quantity

## Common Mapping Scenarios

### Scenario 1: Simple Direct Mapping

**Source Fields:**
- name
- price
- sku
- description

**Mapping:**
```json
{
  "mappings": {
    "direct": {
      "fields": {
        "name": "Title",
        "price": "Variant Price",
        "sku": "Variant SKU",
        "description": "Body (HTML)"
      }
    },
    "default": {
      "fields": {
        "Type": "Product",
        "Published": "TRUE",
        "Vendor": "Default Vendor"
      }
    }
  }
}
```

### Scenario 2: Complex Title from Multiple Fields

**Source Fields:**
- brand
- model
- year

**Mapping:**
```json
{
  "mappings": {
    "concatenate": {
      "fields": {
        "title": {
          "target": "Title",
          "fields": ["brand", "model", "year"],
          "separator": " "
        }
      }
    }
  }
}
```

### Scenario 3: Conditional Status Based on Inventory

**Source Fields:**
- inventory_qty
- status

**Mapping:**
```json
{
  "mappings": {
    "conditional": {
      "fields": {
        "status": {
          "target": "Status",
          "condition": {
            "field": "inventory_qty",
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

## Field Transformation

The transformer automatically handles:

- **Prices**: Converts to 2 decimal places
- **Inventory**: Converts to integer
- **Handles**: Converts to URL-friendly format
- **Tags**: Converts to comma-separated
- **Booleans**: Converts to TRUE/FALSE
- **Image URLs**: Validates and formats
- **HTML**: Cleans and validates

## Tips

1. **Start Simple**: Begin with direct mappings, add complexity as needed
2. **Test Frequently**: Test after each mapping change
3. **Use Defaults**: Set defaults for common fields
4. **Handle Missing Data**: Use conditionals for missing fields
5. **Document Custom Logic**: Comment complex mappings

## Validation

After mapping, the validator checks:

- Required fields are present
- Data types are correct
- Formats match Shopify requirements
- No duplicate SKUs
- Valid URLs and handles

## Example Complete Configuration

```json
{
  "mappings": {
    "direct": {
      "fields": {
        "name": "Title",
        "price": "Variant Price",
        "sku": "Variant SKU",
        "description": "Body (HTML)",
        "vendor": "Vendor",
        "type": "Type"
      }
    },
    "concatenate": {
      "fields": {
        "tags": {
          "target": "Tags",
          "fields": ["category", "brand"],
          "separator": ","
        }
      }
    },
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
    },
    "default": {
      "fields": {
        "Type": "Product",
        "Published": "TRUE",
        "Variant Inventory Tracker": "shopify",
        "Option1 Name": "Size"
      }
    }
  },
  "required_fields": [
    "Handle",
    "Title",
    "Variant SKU",
    "Variant Price"
  ],
  "optional_fields": [
    "Variant Compare At Price",
    "Image Src",
    "SEO Title"
  ]
}
```

---

For migration steps, see `MIGRATION_GUIDE.md`.  
For error handling, see `ERROR_HANDLING.md`.

