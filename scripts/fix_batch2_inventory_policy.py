#!/usr/bin/env python3
"""
Fix inventory policy in batch 2 CSV.
Ensures all variant rows have valid inventory policy values.
"""

import sys
from pathlib import Path
import pandas as pd
from loguru import logger

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def fix_inventory_policy_in_batch(batch_path: str) -> None:
    """
    Fix inventory policy in a batch CSV file.
    
    Args:
        batch_path: Path to the batch CSV file
    """
    logger.info(f"Loading batch file: {batch_path}")
    df = pd.read_csv(batch_path, low_memory=False)
    
    logger.info(f"Total rows: {len(df)}")
    
    # Check current inventory policy values
    if 'Variant Inventory Policy' not in df.columns:
        logger.error("'Variant Inventory Policy' column not found in CSV!")
        return
    
    logger.info("\nBefore fix:")
    logger.info(f"Inventory policy value counts:\n{df['Variant Inventory Policy'].value_counts(dropna=False)}")
    
    # Identify rows that need fixing
    # Rows with variant fields populated must have inventory policy
    fixes = 0
    for idx, row in df.iterrows():
        variant_price = row.get('Variant Price', '')
        variant_sku = row.get('Variant SKU', '')
        variant_inventory = row.get('Variant Inventory Qty', '')
        
        # Check if this row has any variant fields populated
        has_variant_fields = (
            (variant_price and pd.notna(variant_price) and str(variant_price).strip() != '' and str(variant_price).strip() != 'nan') or
            (variant_sku and pd.notna(variant_sku) and str(variant_sku).strip() != '' and str(variant_sku).strip() != 'nan') or
            (variant_inventory and pd.notna(variant_inventory) and str(variant_inventory).strip() != '' and str(variant_inventory).strip() != 'nan' and str(variant_inventory).strip() != '0')
        )
        
        if has_variant_fields:
            policy = row.get('Variant Inventory Policy', '')
            policy_str = str(policy).strip().lower() if pd.notna(policy) else ''
            
            # Check if policy is empty or invalid
            if (pd.isna(policy) or 
                policy_str == '' or 
                policy_str == 'nan' or
                policy_str not in ['deny', 'continue']):
                # Set to 'deny' by default
                df.at[idx, 'Variant Inventory Policy'] = 'deny'
                fixes += 1
    
    logger.info(f"\nFixed {fixes} rows with missing/invalid inventory policy")
    
    logger.info("\nAfter fix:")
    logger.info(f"Inventory policy value counts:\n{df['Variant Inventory Policy'].value_counts(dropna=False)}")
    
    # Verify no empty values remain for rows with variant fields
    empty_count = 0
    for idx, row in df.iterrows():
        variant_price = row.get('Variant Price', '')
        variant_sku = row.get('Variant SKU', '')
        variant_inventory = row.get('Variant Inventory Qty', '')
        
        has_variant_fields = (
            (variant_price and pd.notna(variant_price) and str(variant_price).strip() != '' and str(variant_price).strip() != 'nan') or
            (variant_sku and pd.notna(variant_sku) and str(variant_sku).strip() != '' and str(variant_sku).strip() != 'nan') or
            (variant_inventory and pd.notna(variant_inventory) and str(variant_inventory).strip() != '' and str(variant_inventory).strip() != 'nan' and str(variant_inventory).strip() != '0')
        )
        
        if has_variant_fields:
            policy = row.get('Variant Inventory Policy', '')
            if pd.isna(policy) or str(policy).strip() == '' or str(policy).strip().lower() == 'nan':
                empty_count += 1
    
    if empty_count > 0:
        logger.warning(f"WARNING: {empty_count} rows with variant fields still have empty inventory policy!")
    else:
        logger.info("✓ All rows with variant fields now have valid inventory policy")
    
    # Save the fixed file
    logger.info(f"\nSaving fixed file to: {batch_path}")
    df.to_csv(batch_path, index=False)
    logger.info("✓ File saved successfully!")


if __name__ == "__main__":
    batch_path = "data/output/shopify_products_batch_2_of_5.csv"
    
    if not Path(batch_path).exists():
        logger.error(f"Batch file not found: {batch_path}")
        sys.exit(1)
    
    fix_inventory_policy_in_batch(batch_path)
    logger.info("\n✓ Batch 2 inventory policy fix complete!")


