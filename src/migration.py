"""
Migration Orchestrator Module
Coordinates the entire migration process.
"""

import pandas as pd
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
    
    def migrate(self) -> Dict[str, Any]:
        """
        Execute the complete migration process.
        
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
        
        # Process each row
        shopify_rows = []
        errors = []
        
        logger.info(f"Processing {len(source_df)} rows...")
        
        for idx, row in tqdm(source_df.iterrows(), total=len(source_df), desc="Migrating"):
            row_number = idx + 2  # +2 because CSV rows start at 1, and header is row 1
            
            try:
                # Validate source row
                source_row = row.to_dict()
                is_valid_source, source_errors = self.validator.validate_source_row(source_row, row_number)
                
                if not is_valid_source:
                    errors.append({
                        'row_number': row_number,
                        'type': 'source_validation',
                        'errors': source_errors,
                        'row_data': source_row
                    })
                    self.stats['failed_rows'] += 1
                    continue
                
                # Map fields
                mapped_row = self.mapper.map_row(source_row)
                
                # Generate URL handle from Title if not already set
                if 'Title' in mapped_row and 'URL handle' not in mapped_row:
                    mapped_row['URL handle'] = mapped_row['Title']
                
                # Handle missing price - use Sale price or set default
                if 'Price' not in mapped_row or not mapped_row.get('Price') or pd.isna(mapped_row.get('Price')):
                    # Try to use Sale price if available
                    if 'Compare-at price' in mapped_row and mapped_row.get('Compare-at price'):
                        mapped_row['Price'] = mapped_row['Compare-at price']
                    else:
                        # Skip products without prices (they will fail validation)
                        pass
                
                # Transform data
                transformed_row = self.transformer.transform_row(mapped_row)
                
                # Skip products without Product image URL
                product_image_url = transformed_row.get('Product image URL', '')
                if not product_image_url or pd.isna(product_image_url) or str(product_image_url).strip() == '':
                    errors.append({
                        'row_number': row_number,
                        'type': 'missing_image',
                        'errors': ['Missing Product image URL'],
                        'warnings': [],
                        'row_data': transformed_row
                    })
                    self.stats['failed_rows'] += 1
                    continue
                
                # Validate transformed row
                is_valid, validation_errors, validation_warnings = self.validator.validate_shopify_row(
                    transformed_row,
                    row_number,
                    required_fields
                )
                
                if not is_valid:
                    errors.append({
                        'row_number': row_number,
                        'type': 'validation',
                        'errors': validation_errors,
                        'warnings': validation_warnings,
                        'row_data': transformed_row
                    })
                    self.stats['failed_rows'] += 1
                    if not self._should_continue_on_error():
                        break
                    continue
                
                # Add warnings to stats
                if validation_warnings:
                    self.stats['warnings'].extend([
                        {'row_number': row_number, 'warning': w} for w in validation_warnings
                    ])
                
                # Handle duplicate SKUs by adding suffix (before ensuring all columns)
                if 'SKU' in transformed_row and transformed_row['SKU']:
                    sku = transformed_row['SKU']
                    # Check if this SKU already exists in our output
                    existing_skus = [row.get('SKU', '') for row in shopify_rows]
                    if sku in existing_skus:
                        # Add suffix to make it unique
                        counter = 1
                        new_sku = f"{sku}-{counter}"
                        while new_sku in existing_skus:
                            counter += 1
                            new_sku = f"{sku}-{counter}"
                        transformed_row['SKU'] = new_sku
                        logger.warning(f"Row {row_number}: Duplicate SKU '{sku}' changed to '{new_sku}'")
                
                # Handle fulfillment service based on inventory tracker
                # Shopify rule: 
                # - If inventory tracker is "shopify", fulfillment service should be "manual"
                # - If inventory tracker is "not tracked", fulfillment service must be empty (but Shopify may reject this)
                # Default: Always use "shopify" + "manual" for compatibility
                inventory_tracker = transformed_row.get('Inventory tracker', 'shopify')
                if 'Fulfillment service' in shopify_columns:
                    if inventory_tracker == 'not tracked' or inventory_tracker == '' or str(inventory_tracker).lower() == 'not tracked':
                        # When inventory is not tracked, fulfillment service must be empty
                        # But Shopify may reject this, so we'll set to empty only if explicitly needed
                        transformed_row['Fulfillment service'] = ''
                    else:
                        # Default: If inventory is tracked (shopify), set to 'manual'
                        transformed_row['Fulfillment service'] = 'manual'
                
                # Ensure fulfillment service is always set (default to 'manual' if empty)
                if 'Fulfillment service' in shopify_columns:
                    if not transformed_row.get('Fulfillment service') or transformed_row.get('Fulfillment service') == '':
                        # If empty, default to 'manual' and set inventory tracker to 'shopify'
                        transformed_row['Fulfillment service'] = 'manual'
                        transformed_row['Inventory tracker'] = 'shopify'
                
                # Ensure "Continue selling when out of stock" is set correctly
                # This is the "Inventory policy" - should be "deny" or "continue"
                if 'Continue selling when out of stock' in shopify_columns:
                    if 'Continue selling when out of stock' not in transformed_row or not transformed_row.get('Continue selling when out of stock'):
                        transformed_row['Continue selling when out of stock'] = 'deny'
                
                # Ensure all Shopify columns are present (only add empty if truly missing)
                for col in shopify_columns:
                    if col not in transformed_row:
                        transformed_row[col] = ""
                    # Debug: Check if Description is being lost
                    if col == 'Description' and transformed_row.get(col):
                        # Description exists, make sure it's preserved
                        pass
                
                # Reorder columns to match template (preserve all values)
                ordered_row = {}
                for col in shopify_columns:
                    # Use get() with empty string default, but preserve actual values
                    if col in transformed_row:
                        ordered_row[col] = transformed_row[col]
                    else:
                        ordered_row[col] = ""
                shopify_rows.append(ordered_row)
                
                self.stats['successful_rows'] += 1
                self.stats['processed_rows'] += 1
                
            except Exception as e:
                logger.error(f"Error processing row {row_number}: {e}")
                errors.append({
                    'row_number': row_number,
                    'type': 'processing_error',
                    'error': str(e),
                    'row_data': source_row if 'source_row' in locals() else {}
                })
                self.stats['failed_rows'] += 1
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
            
            # Shopify only accepts specific columns - remove columns that Shopify doesn't recognize
            # Keep only the columns that Shopify actually accepts
            shopify_accepted_columns = [
                'Handle', 'Title', 'Body (HTML)', 'Vendor', 'Type', 'Tags', 'Published',
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

