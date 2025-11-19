#!/bin/bash
# Full Migration Verification Script
# Runs comprehensive verification once migration is complete

OUTPUT_FILE="data/output/shopify_products_FULL_MIGRATION.csv"

echo "=========================================="
echo "FULL MIGRATION VERIFICATION"
echo "=========================================="
echo ""

# Wait for file to be created
echo "Waiting for migration file to be created..."
while [ ! -f "$OUTPUT_FILE" ]; do
    sleep 5
    echo -n "."
done
echo ""
echo "✅ Migration file found!"
echo ""

# Check file size
FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
echo "File size: $FILE_SIZE"
echo ""

# Run comprehensive verification
echo "Running comprehensive verification..."
echo ""
./venv/bin/python scripts/verify_migration.py "$OUTPUT_FILE"

echo ""
echo "=========================================="
echo "Verification complete!"
echo "=========================================="

