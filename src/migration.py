"""
Migration Orchestrator Module
Coordinates the entire migration process.
"""

import pandas as pd
import re
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
from loguru import logger
from tqdm import tqdm

from .csv_handler import CSVHandler
from .mapper import FieldMapper
from .transformer import DataTransformer
from .validator import DataValidator


class MigrationOrchestrator:
    """Orchestrate the complete migration process."""
    
    def __init__(
        self,
        source_csv_path: str,
        shopify_template_path: str,
        output_path: str,
        mapping_config_path: Optional[str] = None,
        sample_size: Optional[int] = None
    ):
        """
        Initialize migration orchestrator.
        
        Args:
            source_csv_path: Path to source products CSV
            shopify_template_path: Path to Shopify template CSV
            output_path: Path for output CSV
            mapping_config_path: Path to field mapping configuration
            sample_size: Optional sample size for testing
        """
        self.source_csv_path = Path(source_csv_path)
        self.shopify_template_path = Path(shopify_template_path)
        self.output_path = Path(output_path)
        self.mapping_config_path = mapping_config_path
        self.sample_size = sample_size
        
        # Initialize components
        self.csv_handler = CSVHandler()
        self.mapper = FieldMapper(mapping_config_path)
        self.transformer = DataTransformer()
        self.validator = DataValidator()
        
        # Statistics
        self.stats = {
            'total_rows': 0,
            'processed_rows': 0,
            'successful_rows': 0,
            'failed_rows': 0,
            'errors': [],
            'warnings': []
        }
    
    def analyze_source(self) -> Dict[str, Any]:
        """
        Analyze the source CSV file.
        
        Returns:
            Analysis dictionary
        """
        logger.info("Analyzing source CSV file...")
        analysis = self.csv_handler.analyze_csv(str(self.source_csv_path))
        self.stats['total_rows'] = analysis['row_count']
        return analysis
    
    def load_shopify_template(self) -> pd.DataFrame:
        """
        Load Shopify template to understand required structure.
        
        Returns:
            Template DataFrame
        """
        logger.info("Loading Shopify template...")
        template_df = self.csv_handler.read_csv(str(self.shopify_template_path))
        logger.info(f"Template has {len(template_df.columns)} columns: {list(template_df.columns)}")
        return template_df
    
    def _extract_base_product_name(self, product_name: str) -> str:
        """Extract base product name, removing variant suffixes like '- Small', '- Medium', etc."""
        if pd.isna(product_name):
            return ""
        
        name = str(product_name).strip()
        # Remove common variant patterns: " - Small", " - Medium", " - Large", etc.
        # Also handle patterns like "-Small", "-Small/Black", etc.
        import re
        # Remove size/color variants at the end
        name = re.sub(r'\s*-\s*(Small|Medium|Large|XLarge|XSmall|XL|XXL|S|M|L|XS|XXS)(\s*/\s*[^,]+)?$', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s*-\s*[A-Z][a-z]+(\s*/\s*[^,]+)?$', '', name)  # Remove other single-word variants
        return name.strip()
    
    def _extract_variant_option(self, product_name: str, base_name: str, sku: str = None) -> str:
        """Extract variant option value from product name or SKU."""
        if pd.isna(product_name) or not base_name:
            return ""
        
        name = str(product_name).strip()
        base = str(base_name).strip()
        
        if name == base:
            # If name is same as base, try to extract from SKU
            if sku and pd.notna(sku):
                sku_str = str(sku).strip()
                # Extract size from SKU patterns like "Black/Black-Small" or "4-Pink"
                # Look for size patterns at the end
                size_match = re.search(r'[-/](Small|Medium|Large|XLarge|XSmall|XL|XXL|S|M|L|XS|XXS|XXS)$', sku_str, re.IGNORECASE)
                if size_match:
                    return size_match.group(1)
                # If no size found, try to extract color or other variant
                # Pattern: "Color-Size" or "Size-Color"
                parts = re.split(r'[-/]', sku_str)
                if len(parts) > 1:
                    # Check if last part is a size
                    last_part = parts[-1].strip()
                    if last_part.lower() in ['small', 'medium', 'large', 'xlarge', 'xsmall', 'xl', 'xxl', 's', 'm', 'l', 'xs', 'xxs']:
                        return last_part
                    # Otherwise return the last part as variant
                    return last_part
                # If SKU is just a number (like "9"), use it as the variant option
                if sku_str.isdigit():
                    return sku_str
            return ""  # Parent product, no variant
        
        # Extract variant part after base name
        if name.startswith(base):
            variant = name[len(base):].strip()
            # Remove leading dash and spaces
            variant = variant.lstrip('-').strip()
            # Handle cases like "Small/Black" - take first part
            if '/' in variant:
                variant = variant.split('/')[0].strip()
            # If variant is just a number (like "- 9"), extract it
            if variant.isdigit():
                return variant
            # Extract number from variant like "- 9" or "-9"
            num_match = re.search(r'[- ]?(\d+)$', variant)
            if num_match:
                return num_match.group(1)
            return variant
        
        return ""
    
    def migrate(self) -> Dict[str, Any]:
        """
        Execute the complete migration process with variant grouping.
        
        Returns:
            Migration statistics dictionary
        """
        logger.info("Starting migration process...")
        
        # Load source data
        source_df = self.csv_handler.read_csv(str(self.source_csv_path))
        
        # Apply sample size if specified
        if self.sample_size and len(source_df) > self.sample_size:
            logger.info(f"Using sample size: {self.sample_size} rows")
            source_df = source_df.head(self.sample_size)
        
        # Load Shopify template to get column structure
        template_df = self.load_shopify_template()
        shopify_columns = list(template_df.columns)
        
        # Get required fields from mapper
        required_fields = self.mapper.get_required_fields()
        self.validator.required_fields = required_fields
        
        # Group products by base name for variant handling
        logger.info("Grouping products by base name for variant handling...")
        source_df['BaseName'] = source_df['Name'].apply(self._extract_base_product_name)
        
        # Process products grouped by base name
        shopify_rows = []
        errors = []
        
        logger.info(f"Processing {len(source_df)} rows, grouped into products...")
        
        # Group by base name
        grouped = source_df.groupby('BaseName')
        
        for base_name, group_df in tqdm(grouped, total=len(grouped), desc="Migrating"):
            try:
                # Find parent product (Type='variable' or has no price/variant info)
                parent_row = None
                variants = []
                
                for idx, row in group_df.iterrows():
                    row_type = str(row.get('Type', '')).lower()
                    has_price = pd.notna(row.get('Regular price')) and str(row.get('Regular price', '')).strip()
                    
                    # Parent is the one with Type='variable' and no price
                    if row_type == 'variable' and not has_price:
                        parent_row = row
                    elif row_type == 'variation' and has_price:
                        # Variants have Type='variation' and have prices
                        variants.append((idx, row))
                    elif not has_price and parent_row is None:
                        # Fallback: if no parent found yet and this has no price, it might be parent
                        parent_row = row
                    else:
                        # Everything else goes to variants
                        variants.append((idx, row))
                
                # If no parent found, use first row as parent
                if parent_row is None and len(group_df) > 0:
                    parent_row = group_df.iloc[0]
                    variants = [(idx, row) for idx, row in group_df.iterrows() if idx != parent_row.name]
                
                if parent_row is None:
                    continue
                
                # Process parent product
                parent_dict = parent_row.to_dict()
                parent_dict['Name'] = base_name  # Use base name for parent
                
                # Map parent fields
                mapped_parent = self.mapper.map_row(parent_dict)
                
                # Generate Handle from base name
                if 'Title' in mapped_parent:
                    mapped_parent['URL handle'] = mapped_parent['Title']
                
                # Transform parent
                transformed_parent = self.transformer.transform_row(mapped_parent)
                
                # Check for image on parent
                parent_image = transformed_parent.get('Product image URL', '')
                if not parent_image or pd.isna(parent_image) or str(parent_image).strip() == '':
                    # Try to get image from variants
                    for _, variant_row in variants:
                        variant_dict = variant_row.to_dict()
                        variant_mapped = self.mapper.map_row(variant_dict)
                        variant_transformed = self.transformer.transform_row(variant_mapped)
                        variant_image = variant_transformed.get('Product image URL', '')
                        if variant_image and pd.notna(variant_image) and str(variant_image).strip():
                            transformed_parent['Product image URL'] = variant_image
                            break
                
                # Skip if still no image
                if not transformed_parent.get('Product image URL') or pd.isna(transformed_parent.get('Product image URL')) or str(transformed_parent.get('Product image URL', '')).strip() == '':
                    errors.append({
                        'row_number': parent_row.name + 2,
                        'type': 'missing_image',
                        'errors': ['Missing Product image URL'],
                        'row_data': transformed_parent
                    })
                    self.stats['failed_rows'] += len(variants) + 1
                    continue
                
                # Get handle for variants
                handle = transformed_parent.get('URL handle', '')
                if not handle:
                    handle = self.transformer._transform_handle(base_name) if base_name else ""
                
                # Create parent row (first row with full info)
                parent_shopify_row = {}
                for col in shopify_columns:
                    if col in transformed_parent:
                        parent_shopify_row[col] = transformed_parent[col]
                    else:
                        parent_shopify_row[col] = ""
                
                # Parent product should NOT have Price, SKU, or variant-specific fields
                # These cause Shopify to treat the parent as a "Default" variant with ₹0.00
                # Clear ALL variant-related fields from parent
                variant_fields = ['Price', 'Compare-at price', 'SKU', 'Variant SKU', 'Variant Price', 
                                 'Variant Compare At Price', 'Variant Inventory Qty', 'Variant Inventory Tracker',
                                 'Variant Inventory Policy', 'Variant Fulfillment Service', 'Variant Grams',
                                 'Variant Requires Shipping', 'Variant Taxable', 'Variant Image']
                for field in variant_fields:
                    if field in parent_shopify_row:
                        parent_shopify_row[field] = ""
                
                # Clean product category - Shopify requires exact taxonomy match
                # Since we don't have Shopify's exact taxonomy, leave it empty to avoid errors
                # User can set categories manually in Shopify after import
                if 'Product category' in parent_shopify_row:
                    # Leave category empty - Shopify will show warning but won't fail import
                    # User can set correct categories in Shopify admin after import
                    parent_shopify_row['Product category'] = ""
                
                # Set Type to empty or valid value (not "variable")
                if 'Type' in parent_shopify_row:
                    type_val = parent_shopify_row.get('Type', '')
                    if pd.isna(type_val) or str(type_val).lower() == 'variable' or str(type_val).lower() == 'nan':
                        parent_shopify_row['Type'] = ""  # Empty for parent with variants
                
                # Set Option1 Name if we have variants (but don't set Option1 Value for parent)
                # IMPORTANT: Only set Option1 Name if we actually have variants to prevent default variant
                if variants and len(variants) > 0:
                    parent_shopify_row['Option1 name'] = 'Size'  # Default, can be improved
                else:
                    # No variants, so don't set Option1 Name (prevents default variant)
                    if 'Option1 name' in parent_shopify_row:
                        parent_shopify_row['Option1 name'] = ""
                # Parent should NOT have Option1 Value (that's only for variants)
                if 'Option1 value' in parent_shopify_row:
                    parent_shopify_row['Option1 value'] = ""
                
                shopify_rows.append(parent_shopify_row)
                self.stats['successful_rows'] += 1
                
                # Process variants
                # Get parent image to copy to variants if they don't have one
                parent_image = transformed_parent.get('Product image URL', '')
                
                for variant_idx, variant_row in variants:
                    variant_dict = variant_row.to_dict()
                    
                    # Skip if this is actually the parent product (Type='variable' and no price)
                    variant_type = str(variant_row.get('Type', '')).lower()
                    variant_has_price = pd.notna(variant_row.get('Regular price')) and str(variant_row.get('Regular price', '')).strip()
                    
                    if variant_type == 'variable' and not variant_has_price:
                        continue  # Skip parent product, don't create a "Default" variant
                    
                    # Map variant fields
                    mapped_variant = self.mapper.map_row(variant_dict)
                    
                    # Transform variant
                    transformed_variant = self.transformer.transform_row(mapped_variant)
                    
                    # Copy parent image to variant if variant doesn't have one
                    variant_image = transformed_variant.get('Product image URL', '')
                    if (not variant_image or pd.isna(variant_image) or str(variant_image).strip() == '') and parent_image:
                        transformed_variant['Product image URL'] = parent_image
                    
                    # Extract variant option value (try from name first, then SKU)
                    variant_option = self._extract_variant_option(
                        str(variant_row.get('Name', '')),
                        base_name,
                        sku=str(variant_row.get('SKU', ''))
                    )
                    
                    # Create variant row (Title empty, same Handle, Option values, Variant Price)
                    variant_shopify_row = {}
                    for col in shopify_columns:
                        if col == 'Title':
                            variant_shopify_row[col] = ""  # Empty for variants
                        elif col == 'URL handle':
                            variant_shopify_row[col] = handle  # Same handle
                        elif col == 'Option1 value':
                            variant_shopify_row[col] = variant_option if variant_option else ""
                        elif col == 'Option1 name':
                            variant_shopify_row[col] = 'Size' if variant_option else ""
                        elif col in transformed_variant:
                            variant_shopify_row[col] = transformed_variant[col]
                        else:
                            variant_shopify_row[col] = ""
                    
                    # Handle inventory AFTER creating the row to override transformer's default
                    # Check source data directly
                    source_stock = variant_row.get('Stock', '')
                    in_stock = variant_row.get('In stock?', 0)
                    
                    # Convert in_stock to comparable value (handle both int and string)
                    in_stock_val = int(in_stock) if pd.notna(in_stock) else 0
                    if isinstance(in_stock, str):
                        in_stock_val = 1 if in_stock.strip() == '1' else 0
                    
                    # Find the inventory quantity column in shopify_columns (before mapping)
                    inventory_col = None
                    for col in shopify_columns:
                        if 'Inventory quantity' in col or ('Inventory' in col and 'quantity' in col.lower()):
                            inventory_col = col
                            break
                    
                    if inventory_col and inventory_col in variant_shopify_row:
                        # Use actual Stock value from source data, or default to 100
                        if pd.notna(source_stock) and str(source_stock).strip() != '':
                            try:
                                stock_val = float(source_stock)
                                # Use actual stock value (even if 0)
                                variant_shopify_row[inventory_col] = str(int(stock_val))
                            except:
                                variant_shopify_row[inventory_col] = str(source_stock).strip()
                        else:
                            # No stock value in source - set default to 100
                            variant_shopify_row[inventory_col] = "100"
                        
                        # CRITICAL: When inventory quantity is set, ensure inventory tracker is "shopify"
                        # Find inventory tracker column
                        tracker_col = None
                        for col in shopify_columns:
                            if 'Inventory tracker' in col or ('Inventory' in col and 'tracker' in col.lower()):
                                tracker_col = col
                                break
                        
                        if tracker_col and tracker_col in variant_shopify_row:
                            # Set to "shopify" to track inventory
                            variant_shopify_row[tracker_col] = "shopify"
                    
                    # Ensure variant has price - skip if no price
                    if not variant_shopify_row.get('Price') or pd.isna(variant_shopify_row.get('Price')) or str(variant_shopify_row.get('Price', '')).strip() == '':
                        continue  # Skip variants without price
                    
                    shopify_rows.append(variant_shopify_row)
                    self.stats['successful_rows'] += 1
                
                self.stats['processed_rows'] += len(group_df)
                
            except Exception as e:
                logger.error(f"Error processing product group '{base_name}': {e}")
                errors.append({
                    'row_number': 'group',
                    'type': 'processing_error',
                    'error': str(e),
                    'row_data': {'base_name': base_name}
                })
                self.stats['failed_rows'] += len(group_df)
                if not self._should_continue_on_error():
                    break
        
        # Create output DataFrame
        if shopify_rows:
            shopify_df = pd.DataFrame(shopify_rows, columns=shopify_columns)
            
            # Map template column names to Shopify's actual accepted column names
            # The template uses different names than what Shopify import accepts
            column_mapping = {
                'URL handle': 'Handle',
                'Description': 'Body (HTML)',
                'Published on online store': 'Published',
                'SKU': 'Variant SKU',
                'Price': 'Variant Price',
                'Compare-at price': 'Variant Compare At Price',
                'Product image URL': 'Image Src',
                'Option1 name': 'Option1 Name',
                'Option1 value': 'Option1 Value',
                'Option2 name': 'Option2 Name',
                'Option2 value': 'Option2 Value',
                'Option3 name': 'Option3 Name',
                'Option3 value': 'Option3 Value',
                'Inventory tracker': 'Variant Inventory Tracker',
                'Inventory quantity': 'Variant Inventory Qty',
                'Continue selling when out of stock': 'Variant Inventory Policy',
                'Fulfillment service': 'Variant Fulfillment Service',
                'Weight value (grams)': 'Variant Grams',
                'Requires shipping': 'Variant Requires Shipping',
                'Charge tax': 'Variant Taxable',
                'Image alt text': 'Image Alt Text',
                'Variant image URL': 'Variant Image',
                'SEO title': 'SEO Title',
                'SEO description': 'SEO Description'
            }
            
            # Apply column mapping (only rename columns that exist)
            rename_dict = {old: new for old, new in column_mapping.items() if old in shopify_df.columns}
            if rename_dict:
                shopify_df = shopify_df.rename(columns=rename_dict)
                logger.info(f"Renamed {len(rename_dict)} columns to Shopify standard format")
            
            # After column mapping, ensure parent rows don't have variant-specific fields
            # This prevents Shopify from creating "Default" variants with ₹0.00
            variant_field_names = [
                'Variant SKU', 'Variant Price', 'Variant Compare At Price', 
                'Variant Inventory Qty', 'Variant Inventory Tracker', 'Variant Inventory Policy',
                'Variant Fulfillment Service', 'Variant Grams', 'Variant Requires Shipping',
                'Variant Taxable', 'Variant Image', 'Variant Barcode',
                'Option1 Value', 'Option2 Value', 'Option3 Value',
                'Option1 Name', 'Option2 Name', 'Option3 Name'
            ]
            
            for idx, row in shopify_df.iterrows():
                # Check if this is a parent row (has Title)
                if pd.notna(row.get('Title')) and str(row.get('Title', '')).strip() != '':
                    # Clear ALL variant-specific fields from parent
                    for field in variant_field_names:
                        if field in shopify_df.columns:
                            shopify_df.at[idx, field] = ""
            
            # Replace any remaining NaN values with empty strings before writing
            shopify_df = shopify_df.fillna('')
            shopify_df = shopify_df.replace('nan', '')
            
            # Ensure all variants with inventory quantity have inventory tracker set to "shopify"
            if 'Variant Inventory Qty' in shopify_df.columns and 'Variant Inventory Tracker' in shopify_df.columns:
                for idx, row in shopify_df.iterrows():
                    # Only process variants (rows without Title)
                    if pd.isna(row.get('Title')) or str(row.get('Title', '')).strip() == '':
                        inventory_qty = row.get('Variant Inventory Qty', '')
                        inventory_tracker = row.get('Variant Inventory Tracker', '')
                        
                        # If inventory quantity is set (not empty), ensure tracker is "shopify"
                        if inventory_qty and str(inventory_qty).strip() != '' and str(inventory_qty).strip() != 'nan':
                            if not inventory_tracker or str(inventory_tracker).strip().lower() != 'shopify':
                                shopify_df.at[idx, 'Variant Inventory Tracker'] = "shopify"
            
            # Shopify only accepts specific columns - remove columns that Shopify doesn't recognize
            # Keep only the columns that Shopify actually accepts
            shopify_accepted_columns = [
                'Handle', 'Title', 'Body (HTML)', 'Vendor', 'Product category', 'Type', 'Tags', 'Published',
                'Option1 Name', 'Option1 Value', 'Option2 Name', 'Option2 Value',
                'Option3 Name', 'Option3 Value', 'Variant SKU', 'Variant Grams',
                'Variant Inventory Tracker', 'Variant Inventory Qty', 'Variant Inventory Policy',
                'Variant Fulfillment Service', 'Variant Price', 'Variant Compare At Price',
                'Variant Requires Shipping', 'Variant Taxable', 'Variant Barcode',
                'Image Src', 'Image Alt Text', 'Variant Image', 'Gift Card',
                'SEO Title', 'SEO Description'
            ]
            
            # Keep only columns that exist in the dataframe and are accepted by Shopify
            columns_to_keep = [col for col in shopify_accepted_columns if col in shopify_df.columns]
            
            # Also keep any columns that start with "Google Shopping" as they might be accepted
            google_shopping_cols = [col for col in shopify_df.columns if col.startswith('Google Shopping')]
            columns_to_keep.extend(google_shopping_cols)
            
            # Select only the accepted columns
            shopify_df = shopify_df[columns_to_keep]
            logger.info(f"Filtered to {len(columns_to_keep)} Shopify-accepted columns (removed {len(shopify_df.columns) - len(columns_to_keep) if len(shopify_df.columns) > len(columns_to_keep) else 0} unrecognized columns)")
            
            # Write output CSV
            self.csv_handler.write_csv(shopify_df, str(self.output_path))
            logger.info(f"Successfully created output file: {self.output_path}")
        else:
            logger.error("No rows were successfully migrated!")
            raise ValueError("Migration failed: No valid rows to output")
        
        # Generate error report if there are errors
        if errors:
            self._generate_error_report(errors)
        
        # Update statistics
        self.stats['errors'] = errors
        self.stats['total_rows'] = len(source_df)
        
        # Generate validation report
        validation_report = self.validator.generate_validation_report(
            source_df,
            shopify_df,
            errors
        )
        
        # Log summary
        self._log_summary(validation_report)
        
        return {
            'statistics': self.stats,
            'validation_report': validation_report,
            'output_file': str(self.output_path)
        }
    
    def _should_continue_on_error(self) -> bool:
        """Check if migration should continue on errors."""
        # This can be configured via config file
        return True
    
    def _generate_error_report(self, errors: List[Dict[str, Any]]) -> None:
        """
        Generate error report CSV.
        
        Args:
            errors: List of error dictionaries
        """
        error_report_path = self.output_path.parent / f"error_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        error_rows = []
        for error in errors:
            error_rows.append({
                'row_number': error.get('row_number', ''),
                'error_type': error.get('type', ''),
                'errors': '; '.join(error.get('errors', [])) if isinstance(error.get('errors'), list) else str(error.get('error', '')),
                'warnings': '; '.join(error.get('warnings', [])) if isinstance(error.get('warnings'), list) else ''
            })
        
        if error_rows:
            error_df = pd.DataFrame(error_rows)
            self.csv_handler.write_csv(error_df, str(error_report_path))
            logger.info(f"Error report saved to: {error_report_path}")
    
    def _log_summary(self, validation_report: Dict[str, Any]) -> None:
        """
        Log migration summary.
        
        Args:
            validation_report: Validation report dictionary
        """
        logger.info("=" * 60)
        logger.info("MIGRATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total source rows: {self.stats['total_rows']}")
        logger.info(f"Successfully migrated: {self.stats['successful_rows']}")
        logger.info(f"Failed rows: {self.stats['failed_rows']}")
        logger.info(f"Error rate: {validation_report.get('error_rate', 0):.2f}%")
        logger.info(f"Warnings: {len(self.stats['warnings'])}")
        logger.info(f"Output file: {self.output_path}")
        logger.info("=" * 60)

