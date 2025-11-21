"""
Migration Orchestrator Module
Coordinates the entire migration process.
"""

import pandas as pd
import re
from pathlib import Path
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation
from datetime import datetime
from loguru import logger
from tqdm import tqdm

from .csv_handler import CSVHandler
from .mapper import FieldMapper
from .transformer import DataTransformer
from .validator import DataValidator


def normalize_product_name(product_name: str) -> str:
    """
    Normalize product name by stripping variant suffixes (size, color, numeric).
    This helper is shared between migration logic and batch splitting to ensure consistency.
    """
    if pd.isna(product_name):
        return ""
    
    name = str(product_name).strip()
    colors = [
        'Black', 'White', 'Red', 'Blue', 'Green', 'Yellow', 'Orange', 'Pink', 'Purple',
        'Brown', 'Grey', 'Gray', 'Silver', 'Gold', 'Navy', 'Teal', 'Cyan', 'Magenta',
        'Beige', 'Tan', 'Maroon', 'Olive', 'Lime', 'Aqua', 'Coral', 'Salmon', 'Khaki',
        'Burgundy', 'Charcoal', 'Cream', 'Ivory', 'Mint', 'Peach', 'Turquoise', 'Violet',
        'Amber', 'Bronze', 'Copper', 'Indigo', 'Lavender', 'Mauve', 'Mustard', 'Plum',
        'Rose', 'Ruby', 'Sage', 'Scarlet', 'Taupe', 'Wine', 'Azure', 'Champagne'
    ]
    
    sizes = [
        'Small', 'Medium', 'Large', 'XLarge', 'XSmall', 'XL', 'XXL', 'S', 'M', 'L', 'XS', 'XXS',
        'Extra Small', 'Extra Large', '2XL', '3XL', '4XL', '5XL', 'XXXL', 'XXXXL',
        'Petite', 'Regular', 'Tall', 'Short', 'Plus', 'Oversized'
    ]
    
    size_pattern = r'\s*[-(\s]*(' + '|'.join(re.escape(s) for s in sizes) + r')(\s*[/)]\s*[^,]+)?\s*[)\s]*$'
    color_pattern = r'\s*[-(\s]*(' + '|'.join(re.escape(c) for c in colors) + r')(\s*[/)]\s*[^,]+)?\s*[)\s]*$'
    number_pattern = r'\s*[-(\s]*(\d+\.?\d*)\s*[)\s]*$'
    size_color_pattern = r'\s*-\s*(' + '|'.join(re.escape(s) for s in sizes) + r')\s*/\s*(' + '|'.join(re.escape(c) for c in colors) + r')\s*$'
    color_size_pattern = r'\s*-\s*(' + '|'.join(re.escape(c) for c in colors) + r')\s*/\s*(' + '|'.join(re.escape(s) for s in sizes) + r')\s*$'
    word_pattern = r'\s*-\s*[A-Z][a-z]+(\s*/\s*[A-Z][a-z]+)?\s*$'
    paren_pattern = r'\s*\([^)]+\)\s*$'
    
    previous = None
    while previous != name:
        previous = name
        name = re.sub(size_pattern, '', name, flags=re.IGNORECASE)
        name = re.sub(color_pattern, '', name, flags=re.IGNORECASE)
        name = re.sub(number_pattern, '', name)
        name = re.sub(size_color_pattern, '', name, flags=re.IGNORECASE)
        name = re.sub(color_size_pattern, '', name, flags=re.IGNORECASE)
        name = re.sub(word_pattern, '', name)
        name = re.sub(paren_pattern, '', name)
    
    name = name.strip().rstrip('-').strip().rstrip('(').strip()
    return name


def determine_product_group_id(row: pd.Series) -> str:
    """
    Determine a stable product group identifier using parent slug when available,
    otherwise falling back to normalized product name/SKU.
    """
    if row is None:
        return ""
    
    parent_value = row.get('Parent')
    if parent_value is not None:
        parent_str = str(parent_value).strip()
        if parent_str and parent_str.lower() not in ['nan', 'none', '0']:
            return parent_str
    
    row_type = str(row.get('Type', '')).strip().lower()
    sku_value = row.get('SKU', '')
    if sku_value is not None:
        sku_str = str(sku_value).strip()
    else:
        sku_str = ''
    
    if row_type == 'variation':
        # Variants without parent fallback to SKU
        if sku_str and sku_str.lower() not in ['nan', 'none']:
            return sku_str
    
    if row_type in ['variable', 'simple', 'grouped'] and sku_str and sku_str.lower() not in ['nan', 'none']:
        return sku_str
    
    base_name = normalize_product_name(row.get('Name', ''))
    if base_name:
        return base_name
    
    name = row.get('Name', '')
    if pd.notna(name):
        return str(name).strip()
    
    return ""


def clean_price_value(value: Any) -> Optional[str]:
    """
    Normalize a price value from the source row to a Shopify-compatible string.
    Returns None if value is missing, non-numeric, or <= 0.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    
    value_str = str(value).strip()
    if not value_str or value_str.lower() in ['nan', 'none', 'null', 'backorder', 'backordered']:
        return None
    
    cleaned = value_str.replace('$', '').replace(',', '').replace('₹', '').strip()
    if not cleaned:
        return None
    
    try:
        price_decimal = Decimal(cleaned)
    except InvalidOperation:
        return None
    
    if price_decimal <= 0:
        return None
    
    return f"{price_decimal.quantize(Decimal('0.01'))}"
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
        
        # Load category mapping path from config if available
        category_mapping_path = None
        try:
            import yaml
            config_file = Path(__file__).parent.parent / "config" / "config.yaml"
            if config_file.exists():
                with open(config_file, 'r') as f:
                    config = yaml.safe_load(f)
                    category_mapping_path = config.get('files', {}).get('category_mapping')
                    if not category_mapping_path:
                        # Default to category_mapping.json in config directory
                        category_mapping_path = str(Path(__file__).parent.parent / "config" / "category_mapping.json")
        except Exception as e:
            logger.debug(f"Could not load config for category mapping: {e}")
            # Default to category_mapping.json in config directory
            category_mapping_path = str(Path(__file__).parent.parent / "config" / "category_mapping.json")
        
        self.transformer = DataTransformer(category_mapping_path=category_mapping_path)
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
    
    def _extract_source_price(self, row: pd.Series) -> Optional[str]:
        """
        Extract the best available price from the source row.
        Prefers sale price over regular price, then other known price fields.
        """
        price_fields = [
            'Sale price',
            'Regular price',
            'Price',
            'Meta: _sale_price',
            'Meta: _regular_price',
            'Meta: _price'
        ]
        
        for field in price_fields:
            if field in row:
                price = clean_price_value(row.get(field))
                if price:
                    return price
        return None
    
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
        """
        Extract base product name, removing variant suffixes like '- Small', '- Medium', '- 10', color names, etc.
        This ensures products with same base name but different colors/sizes are grouped together.
        IMPROVED: Better detection of color and size variants.
        """
        return normalize_product_name(product_name)
    
    def _extract_variant_option(self, product_name: str, base_name: str, sku: str = None) -> str:
        """
        Extract variant option value from product name or SKU.
        Handles size, color, and other variant information.
        """
        if pd.isna(product_name) or not base_name:
            return ""
        
        name = str(product_name).strip()
        base = str(base_name).strip()
        
        # If name is same as base, extract variant from SKU
        if name == base:
            if sku and pd.notna(sku):
                sku_str = str(sku).strip()
                
                # Pattern 1: Size in SKU like "Black/Black-Small", "Small", "Medium"
                size_match = re.search(r'[-/](Small|Medium|Large|XLarge|XSmall|XL|XXL|S|M|L|XS|XXS)$', sku_str, re.IGNORECASE)
                if size_match:
                    return size_match.group(1)
                
                # Pattern 2: Color-Size or Size-Color like "4-Pink", "5-Green", "Black-Small"
                parts = re.split(r'[-/]', sku_str)
                if len(parts) > 1:
                    last_part = parts[-1].strip()
                    # Check if last part is a size
                    if last_part.lower() in ['small', 'medium', 'large', 'xlarge', 'xsmall', 'xl', 'xxl', 's', 'm', 'l', 'xs', 'xxs']:
                        return last_part
                    # Check if it's a color (common colors)
                    colors = ['black', 'white', 'red', 'blue', 'green', 'yellow', 'orange', 'pink', 'purple',
                              'brown', 'grey', 'gray', 'silver', 'gold', 'navy', 'teal', 'cyan', 'magenta']
                    if last_part.lower() in colors:
                        return last_part.capitalize()
                    # Otherwise return the last part as variant (could be color or other attribute)
                    return last_part
                
                # Pattern 3: Just a number like "9" - use it as variant
                if sku_str.isdigit():
                    return sku_str
                
                # Pattern 4: If SKU itself is a size/color (like "Small", "Medium", "Pink")
                if sku_str.lower() in ['small', 'medium', 'large', 'xlarge', 'xsmall', 'xl', 'xxl', 's', 'm', 'l', 'xs', 'xxs']:
                    return sku_str
                colors_lower = ['black', 'white', 'red', 'blue', 'green', 'yellow', 'orange', 'pink', 'purple',
                               'brown', 'grey', 'gray', 'silver', 'gold', 'navy', 'teal', 'cyan', 'magenta']
                if sku_str.lower() in colors_lower:
                    return sku_str.capitalize()
            
            return ""  # Parent product, no variant
        
        # Extract variant part from name suffix (e.g., "Product Name - Small" or "Product Name - 10")
        if name.startswith(base):
            variant = name[len(base):].strip()
            # Remove leading dash and spaces
            variant = variant.lstrip('-').strip()
            
            if not variant:
                return ""
            
            # Handle cases like "Small/Black" - prefer size over color
            if '/' in variant:
                parts = variant.split('/')
                # Check if first part is a size
                first_part = parts[0].strip()
                if first_part.lower() in ['small', 'medium', 'large', 'xlarge', 'xsmall', 'xl', 'xxl', 's', 'm', 'l', 'xs', 'xxs']:
                    return first_part
                # Otherwise return first part
                return first_part
            
            # Check if variant is a numeric size (like "10", "10.5")
            num_match = re.match(r'^(\d+\.?\d*)$', variant)
            if num_match:
                return variant  # Return as-is (e.g., "10", "10.5")
            
            # If variant is just a number (like "9"), extract it
            if variant.isdigit():
                return variant
            
            # Extract number from variant like "- 9" or "-9" or "- 10.5"
            num_match = re.search(r'[- ]?(\d+\.?\d*)$', variant)
            if num_match:
                return num_match.group(1)
            
            # Check if variant matches common sizes (case-insensitive)
            sizes_lower = ['small', 'medium', 'large', 'xlarge', 'xsmall', 'xl', 'xxl', 's', 'm', 'l', 'xs', 'xxs']
            if variant.lower() in sizes_lower:
                # Return with proper capitalization
                size_map = {
                    'small': 'Small', 'medium': 'Medium', 'large': 'Large',
                    'xlarge': 'XLarge', 'xsmall': 'XSmall', 'xl': 'XL',
                    'xxl': 'XXL', 's': 'S', 'm': 'M', 'l': 'L', 'xs': 'XS', 'xxs': 'XXS'
                }
                return size_map.get(variant.lower(), variant)
            
            # Return the variant as-is (could be size, color, or other)
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
        logger.info("Grouping products by base name/parent for variant handling...")
        source_df['BaseName'] = source_df['Name'].apply(normalize_product_name)
        source_df['ProductGroupID'] = source_df.apply(determine_product_group_id, axis=1)
        
        # Process products grouped by base name
        shopify_rows = []
        errors = []
        
        logger.info(f"Processing {len(source_df)} rows, grouped into products...")
        
        # Group by base name - CRITICAL: Products with same base name MUST be grouped together
        # Even if they have identical names, they should be variants if they have different SKUs or other attributes
        grouped = source_df.groupby('ProductGroupID')
        
        for group_id, group_df in tqdm(grouped, total=len(grouped), desc="Migrating"):
            try:
                base_name = ""
                if 'BaseName' in group_df.columns:
                    base_name = str(group_df['BaseName'].iloc[0]).strip()
                if not base_name:
                    base_name = str(group_id).strip() if group_id else str(group_df['Name'].iloc[0])
                
                # CRITICAL FIX: If multiple rows have the same exact Name (not just base name),
                # they MUST be variants of the same product, not separate products
                # Find parent product (Type='variable' or has no price/variant info)
                parent_row = None
                variants = []
                
                # IMPROVED VARIANT DETECTION LOGIC
                # Strategy: If multiple rows share the same base name, they are likely variants
                # unless they have significantly different names (more than just color/size)
                
                unique_names = group_df['Name'].unique()
                
                # Check if names are similar enough to be variants (same base name)
                # If base names match, they should be variants regardless of exact name match
                if len(group_df) > 1:
                    # Multiple rows with same base name = likely variants
                    # Find parent: prefer row with image, then row with Type='variable', then first row
                    for idx, row in group_df.iterrows():
                        row_image = row.get('Images', '')
                        has_image = pd.notna(row_image) and str(row_image).strip() != '' and str(row_image).strip() != 'nan'
                        row_type = str(row.get('Type', '')).lower()
                        has_price = pd.notna(row.get('Regular price')) and str(row.get('Regular price', '')).strip()
                        
                        # Priority 1: Row with image and Type='variable' or no price
                        if has_image and (row_type == 'variable' or not has_price) and parent_row is None:
                            parent_row = row
                        # Priority 2: Row with image
                        elif has_image and parent_row is None:
                            parent_row = row
                        # Priority 3: Row with Type='variable' and no price
                        elif row_type == 'variable' and not has_price and parent_row is None:
                            parent_row = row
                    
                    # If no parent found yet, use first row
                    if parent_row is None:
                        parent_row = group_df.iloc[0]
                    
                    # All other rows are variants
                    variants = [(idx, row) for idx, row in group_df.iterrows() if idx != parent_row.name]
                    
                    # If only one row, it's a single product (no variants)
                    if len(group_df) == 1:
                        # Single row = single product
                        parent_row = group_df.iloc[0]
                        variants = []
                
                if parent_row is None:
                    continue
                
                # Process parent product
                parent_dict = parent_row.to_dict()
                parent_dict['Name'] = base_name  # Use base name for parent
                
                # CRITICAL: Ensure parent has images - preserve from source row
                # Check if parent row has Images field and preserve it
                parent_images_source = parent_row.get('Images', '')
                # Also check all rows in group for images (in case parent row doesn't have it)
                if not parent_images_source or pd.isna(parent_images_source) or str(parent_images_source).strip() == '' or str(parent_images_source).strip().lower() == 'nan':
                    # Try to get images from any row in the group
                    for _, check_row in group_df.iterrows():
                        check_images = check_row.get('Images', '')
                        if check_images and pd.notna(check_images) and str(check_images).strip() != '' and str(check_images).strip().lower() != 'nan':
                            parent_images_source = str(check_images).strip()
                            break
                
                if parent_images_source and pd.notna(parent_images_source) and str(parent_images_source).strip() != '' and str(parent_images_source).strip().lower() != 'nan':
                    # Ensure Images field is in parent_dict (it should be from to_dict(), but make sure)
                    parent_dict['Images'] = str(parent_images_source).strip()
                    logger.debug(f"Preserved images for parent: {base_name} - {str(parent_images_source)[:50]}...")
                
                # CRITICAL: Ensure parent has description - check Description and Short description
                # If parent doesn't have description, try to get it from variants
                parent_desc = parent_dict.get('Description', '')
                parent_short_desc = parent_dict.get('Short description', '')
                if (not parent_desc or pd.isna(parent_desc) or str(parent_desc).strip() == '') and \
                   (not parent_short_desc or pd.isna(parent_short_desc) or str(parent_short_desc).strip() == ''):
                    # Parent has no description, try to get from variants
                    for _, variant_row in variants:
                        variant_desc = variant_row.get('Description', '')
                        variant_short_desc = variant_row.get('Short description', '')
                        if variant_desc and pd.notna(variant_desc) and str(variant_desc).strip() != '':
                            parent_dict['Description'] = str(variant_desc).strip()
                            break
                        elif variant_short_desc and pd.notna(variant_short_desc) and str(variant_short_desc).strip() != '':
                            parent_dict['Short description'] = str(variant_short_desc).strip()
                            break
                
                # Map parent fields
                mapped_parent = self.mapper.map_row(parent_dict)
                
                # CRITICAL: Double-check that images were mapped - if not, add them directly
                if 'Product image URL' not in mapped_parent or not mapped_parent.get('Product image URL') or pd.isna(mapped_parent.get('Product image URL')):
                    # Images weren't mapped, add them directly
                    if parent_images_source and pd.notna(parent_images_source) and str(parent_images_source).strip() != '' and str(parent_images_source).strip().lower() != 'nan':
                        mapped_parent['Product image URL'] = str(parent_images_source).strip()
                        logger.debug(f"Directly added images to parent product: {base_name}")
                
                # Generate Handle from base name
                if 'Title' in mapped_parent:
                    mapped_parent['URL handle'] = mapped_parent['Title']
                
                # Transform parent
                transformed_parent = self.transformer.transform_row(mapped_parent)
                
                # Check for image on parent - handle multiple images
                parent_image_str = transformed_parent.get('Product image URL', '')
                
                # CRITICAL: If images were lost during transformation, restore them from source
                if not parent_image_str or pd.isna(parent_image_str) or str(parent_image_str).strip() == '' or str(parent_image_str).strip().lower() == 'nan':
                    # Try to restore from parent_images_source (we saved it earlier)
                    if parent_images_source and pd.notna(parent_images_source) and str(parent_images_source).strip() != '' and str(parent_images_source).strip().lower() != 'nan':
                        parent_image_str = str(parent_images_source).strip()
                        transformed_parent['Product image URL'] = parent_image_str
                        logger.debug(f"Restored images after transformation for: {base_name}")
                
                # If still no image, try to get from variants
                if not parent_image_str or pd.isna(parent_image_str) or str(parent_image_str).strip() == '' or str(parent_image_str).strip().lower() == 'nan':
                    for _, variant_row in variants:
                        variant_dict = variant_row.to_dict()
                        variant_mapped = self.mapper.map_row(variant_dict)
                        variant_transformed = self.transformer.transform_row(variant_mapped)
                        variant_image = variant_transformed.get('Product image URL', '')
                        if variant_image and pd.notna(variant_image) and str(variant_image).strip() and str(variant_image).strip().lower() != 'nan':
                            parent_image_str = variant_image
                            transformed_parent['Product image URL'] = parent_image_str
                            break
                
                # Parse multiple images (comma-separated)
                parent_images = []
                if parent_image_str and pd.notna(parent_image_str) and str(parent_image_str).strip() != '':
                    # Split by comma and clean up
                    image_urls = [url.strip() for url in str(parent_image_str).split(',') if url.strip()]
                    parent_images = [url for url in image_urls if url and url != 'nan']
                
                # FIX: If parent has no image, try to get from variants (don't skip entire product group)
                if not parent_images:
                    # Try to get image from variants
                    for _, variant_row in variants:
                        variant_dict = variant_row.to_dict()
                        variant_mapped = self.mapper.map_row(variant_dict)
                        variant_transformed = self.transformer.transform_row(variant_mapped)
                        variant_image = variant_transformed.get('Product image URL', '')
                        if variant_image and pd.notna(variant_image) and str(variant_image).strip() != '':
                            # Split by comma and clean up
                            image_urls = [url.strip() for url in str(variant_image).split(',') if url.strip()]
                            parent_images = [url for url in image_urls if url and url != 'nan']
                            if parent_images:
                                logger.info(f"Using variant image for parent product: {base_name}")
                                break
                
                # CRITICAL: Skip products without images
                if not parent_images:
                    logger.warning(f"Skipping product {base_name} - no images found in parent or variants")
                    errors.append({
                        'row_number': parent_row.name + 2,
                        'type': 'missing_image',
                        'errors': ['Missing Product image URL'],
                        'row_data': {'base_name': base_name}
                    })
                    self.stats['failed_rows'] += len(variants) + 1
                    continue
                
                # Get handle for variants
                handle = transformed_parent.get('URL handle', '')
                if not handle:
                    handle = self.transformer._transform_handle(base_name) if base_name else ""
                
                # Create parent row (first row with full info) - use first image
                parent_shopify_row = {}
                for col in shopify_columns:
                    if col in transformed_parent:
                        parent_shopify_row[col] = transformed_parent[col]
                    else:
                        # CRITICAL: Check for description field mapping
                        # Transformer may create "Body (HTML)" or "Description" depending on field name
                        # Template may have "Description" or "Body (HTML)" - map between them
                        if col == 'Description':
                            # Template has "Description", check if transformer created "Body (HTML)"
                            if 'Body (HTML)' in transformed_parent:
                                parent_shopify_row[col] = transformed_parent['Body (HTML)']
                            elif 'Description' in transformed_parent:
                                parent_shopify_row[col] = transformed_parent['Description']
                            else:
                                parent_shopify_row[col] = ""
                        elif col == 'Body (HTML)':
                            # Template has "Body (HTML)", check if transformer created "Description"
                            if 'Description' in transformed_parent:
                                parent_shopify_row[col] = transformed_parent['Description']
                            elif 'Body (HTML)' in transformed_parent:
                                parent_shopify_row[col] = transformed_parent['Body (HTML)']
                            else:
                                parent_shopify_row[col] = ""
                        else:
                            # For other columns not in transformed_parent, set empty
                            parent_shopify_row[col] = ""
                
                # CRITICAL: Ensure Description/Body (HTML) is set - if still empty, try to get from variants
                desc_col = None
                for col in ['Description', 'Body (HTML)']:
                    if col in shopify_columns:
                        desc_col = col
                        break
                
                if desc_col and (desc_col not in parent_shopify_row or not parent_shopify_row.get(desc_col) or str(parent_shopify_row.get(desc_col, '')).strip() == ''):
                    # Description is still empty, try to get from variants (check ALL variants, not just first)
                    for _, variant_row in variants:
                        variant_dict = variant_row.to_dict()
                        variant_mapped = self.mapper.map_row(variant_dict)
                        variant_transformed = self.transformer.transform_row(variant_mapped)
                        variant_desc = variant_transformed.get('Description') or variant_transformed.get('Body (HTML)', '')
                        if variant_desc and pd.notna(variant_desc) and str(variant_desc).strip() != '' and str(variant_desc).strip().lower() != 'nan':
                            parent_shopify_row[desc_col] = str(variant_desc).strip()
                            break
                    # If still empty after checking variants, try to get from group_df (all rows in group)
                    if desc_col not in parent_shopify_row or not parent_shopify_row.get(desc_col) or str(parent_shopify_row.get(desc_col, '')).strip() == '':
                        for idx, row in group_df.iterrows():
                            if idx == parent_row.name:
                                continue
                            row_desc = row.get('Description', '')
                            row_short_desc = row.get('Short description', '')
                            if row_desc and pd.notna(row_desc) and str(row_desc).strip() != '' and str(row_desc).strip().lower() != 'nan':
                                parent_dict_temp = row.to_dict()
                                parent_dict_temp['Name'] = base_name
                                mapped_temp = self.mapper.map_row(parent_dict_temp)
                                transformed_temp = self.transformer.transform_row(mapped_temp)
                                temp_desc = transformed_temp.get('Description') or transformed_temp.get('Body (HTML)', '')
                                if temp_desc and pd.notna(temp_desc) and str(temp_desc).strip() != '':
                                    parent_shopify_row[desc_col] = str(temp_desc).strip()
                                    break
                            elif row_short_desc and pd.notna(row_short_desc) and str(row_short_desc).strip() != '' and str(row_short_desc).strip().lower() != 'nan':
                                parent_dict_temp = row.to_dict()
                                parent_dict_temp['Name'] = base_name
                                mapped_temp = self.mapper.map_row(parent_dict_temp)
                                transformed_temp = self.transformer.transform_row(mapped_temp)
                                temp_desc = transformed_temp.get('Description') or transformed_temp.get('Body (HTML)', '')
                                if temp_desc and pd.notna(temp_desc) and str(temp_desc).strip() != '':
                                    parent_shopify_row[desc_col] = str(temp_desc).strip()
                                    break
                
                # If description is still empty, leave it empty (no default text)
                # Description will remain blank if not found in source
                
                # Set first image for parent row (if available)
                # CRITICAL: Ensure Product image URL column exists in parent_shopify_row
                # Check both 'Product image URL' and 'Image Src' (template might use either)
                image_col = None
                for col in ['Product image URL', 'Image Src']:
                    if col in shopify_columns:
                        image_col = col
                        break
                
                # If column not found in shopify_columns, add it anyway (Shopify needs it)
                if not image_col:
                    image_col = 'Product image URL'
                
                # Set the image value
                if parent_images:
                    parent_shopify_row[image_col] = parent_images[0]
                    logger.debug(f"Set image for {base_name}: {parent_images[0][:50]}...")
                else:
                    parent_shopify_row[image_col] = ""  # Allow products without images
                
                # CRITICAL: Ensure ALL products have a category (use default if no mapping found)
                # Categories are mapped from source "Categories" field to "Product category"
                # Shopify accepts categories in format: "Category > Subcategory" or just "Category"
                default_cat = getattr(self.transformer, 'default_category', '')
                fallback_strategy = getattr(self.transformer, 'fallback_strategy', 'clear')
                
                # Find the Product category column name
                product_category_col = None
                for col in shopify_columns:
                    if 'Product category' in col or ('category' in col.lower() and 'Product' in col):
                        product_category_col = col
                        break
                
                # Ensure Product category field exists and has a value
                if product_category_col:
                    category_value = parent_shopify_row.get(product_category_col, '')
                    # If category is empty or missing, apply default category
                    if not category_value or str(category_value).strip() == '' or str(category_value).strip().lower() == 'nan':
                        if fallback_strategy == 'default' and default_cat and str(default_cat).strip() != '':
                            parent_shopify_row[product_category_col] = str(default_cat).strip()
                        else:
                            # Even if fallback is 'clear', ensure field exists (empty string)
                            parent_shopify_row[product_category_col] = ""
                
                # Set Type to empty or valid value (not "variable")
                if 'Type' in parent_shopify_row:
                    type_val = parent_shopify_row.get('Type', '')
                    if pd.isna(type_val) or str(type_val).lower() == 'variable' or str(type_val).lower() == 'nan':
                        parent_shopify_row['Type'] = ""  # Empty for parent with variants
                
                # CRITICAL FIX: Handle single products (no variants) differently
                # Single products need variant fields populated, not cleared!
                # Check FIRST before clearing variant fields
                if not variants or len(variants) == 0:
                    # This is a SINGLE PRODUCT (no variants)
                    # For single products, Shopify needs variant fields populated
                    
                    parent_price = self._extract_source_price(parent_row)
                    
                    # CRITICAL: Skip single products with zero or missing prices
                    if not parent_price:
                        logger.warning(f"Skipping single product {base_name} - no price in source data")
                        errors.append({
                            'row_number': parent_row.name + 2,
                            'type': 'missing_price',
                            'errors': ['Missing price for single product'],
                            'row_data': {'base_name': base_name}
                        })
                        self.stats['failed_rows'] += 1
                        continue
                    
                    # Set price in Variant Price field for single products
                    if 'Price' in parent_shopify_row:
                        parent_shopify_row['Price'] = parent_price
                    if 'Variant Price' in parent_shopify_row:
                        parent_shopify_row['Variant Price'] = parent_price
                    logger.debug(f"Set price {parent_price} for single product {base_name}")
                    
                    # Get SKU from source row
                    parent_sku = parent_row.get('SKU', '')
                    if not parent_sku or pd.isna(parent_sku) or str(parent_sku).strip() == '':
                        parent_sku = transformed_parent.get('SKU', '')
                    
                    if parent_sku and pd.notna(parent_sku) and str(parent_sku).strip() != '':
                        if 'SKU' in parent_shopify_row:
                            parent_shopify_row['SKU'] = str(parent_sku).strip()
                        if 'Variant SKU' in parent_shopify_row:
                            parent_shopify_row['Variant SKU'] = str(parent_sku).strip()
                    
                    # Get inventory from parent row
                    parent_stock = parent_row.get('Stock', '')
                    parent_in_stock = parent_row.get('In stock?', 0)
                    
                    # Calculate inventory quantity
                    inventory_value = "100"  # Default when in stock
                    if pd.notna(parent_stock) and str(parent_stock).strip() != '' and str(parent_stock).strip() != 'nan':
                        try:
                            stock_val = float(parent_stock)
                            if stock_val == 0:
                                inventory_value = "10"
                            else:
                                inventory_value = str(int(stock_val))
                        except:
                            if str(parent_stock).strip() == "0":
                                inventory_value = "10"
                    else:
                        in_stock_val = int(parent_in_stock) if pd.notna(parent_in_stock) else 0
                        if isinstance(parent_in_stock, str):
                            in_stock_val = 1 if parent_in_stock.strip() == '1' else 0
                        if in_stock_val == 1:
                            inventory_value = "100"
                        else:
                            inventory_value = "10"
                    
                    # Set inventory quantity in all possible column names
                    for col in shopify_columns:
                        if 'Inventory quantity' in col or ('Inventory' in col and 'quantity' in col.lower() and 'tracker' not in col.lower()):
                            if col in parent_shopify_row:
                                parent_shopify_row[col] = inventory_value
                    
                    # CRITICAL: Set inventory tracker to "shopify" for single products with inventory
                    if inventory_value and str(inventory_value).strip() != '' and str(inventory_value).strip() != '0':
                        for col in shopify_columns:
                            if 'Inventory tracker' in col or ('Inventory' in col and 'tracker' in col.lower()):
                                if col in parent_shopify_row:
                                    parent_shopify_row[col] = "shopify"
                                    break
                    
                    # CRITICAL: Set fulfillment service for single products (Shopify requirement)
                    # Shopify does not allow blank fulfillment service
                    # Use exact column names that will exist after mapping
                    if 'Fulfillment service' in parent_shopify_row:
                        parent_shopify_row['Fulfillment service'] = "manual"
                    if 'Variant Fulfillment Service' in parent_shopify_row:
                        parent_shopify_row['Variant Fulfillment Service'] = "manual"
                    
                    # CRITICAL: Set inventory policy for single products (Shopify requirement)
                    # Shopify accepts only "deny" or "continue"
                    # Use exact column names that will exist after mapping
                    if 'Continue selling when out of stock' in parent_shopify_row:
                        parent_shopify_row['Continue selling when out of stock'] = "deny"
                    if 'Variant Inventory Policy' in parent_shopify_row:
                        parent_shopify_row['Variant Inventory Policy'] = "deny"
                    
                    # Don't set Option1 Name/Value for single products (prevents default variant)
                    if 'Option1 name' in parent_shopify_row:
                        parent_shopify_row['Option1 name'] = ""
                    if 'Option1 value' in parent_shopify_row:
                        parent_shopify_row['Option1 value'] = ""
                    
                    # FINAL CHECK: Ensure single product has valid price before adding
                    # Check all possible price fields in parent_shopify_row
                    final_price_check = None
                    price_fields_to_check = ['Price', 'Variant Price', 'Regular price']
                    
                    for price_field in price_fields_to_check:
                        if price_field in parent_shopify_row:
                            price_val = parent_shopify_row[price_field]
                            # Check if price is not empty, not NaN, and not 'nan' string
                            if price_val is not None and pd.notna(price_val) and str(price_val).strip() != '' and str(price_val).strip().lower() != 'nan':
                                try:
                                    price_float = float(str(price_val).replace('₹', '').replace(',', '').replace('$', '').strip())
                                    if price_float > 0:
                                        final_price_check = price_float
                                        logger.debug(f"Final price check passed for {base_name}: {price_float} from {price_field}")
                                        break
                                except Exception as e:
                                    logger.debug(f"Could not parse price from {price_field} for {base_name}: {e}")
                    
                    # If still no valid price, also check transformed_parent as fallback
                    if not final_price_check or final_price_check <= 0:
                        for price_field in price_fields_to_check:
                            if price_field in transformed_parent:
                                price_val = transformed_parent[price_field]
                                if price_val is not None and pd.notna(price_val) and str(price_val).strip() != '' and str(price_val).strip().lower() != 'nan':
                                    try:
                                        price_float = float(str(price_val).replace('₹', '').replace(',', '').replace('$', '').strip())
                                        if price_float > 0:
                                            # Set it in parent_shopify_row
                                            if 'Variant Price' in parent_shopify_row:
                                                parent_shopify_row['Variant Price'] = f"{price_float:.2f}"
                                            if 'Price' in parent_shopify_row:
                                                parent_shopify_row['Price'] = f"{price_float:.2f}"
                                            final_price_check = price_float
                                            logger.debug(f"Final price check passed (from transformed) for {base_name}: {price_float}")
                                            break
                                    except:
                                        pass
                    
                    if not final_price_check or final_price_check <= 0:
                        logger.warning(f"Skipping single product {base_name} - final price check failed (no valid price set in any field)")
                        errors.append({
                            'row_number': parent_row.name + 2,
                            'type': 'zero_price_final_check',
                            'errors': ['Single product has no valid price after processing'],
                            'row_data': {'base_name': base_name, 'price_fields_checked': price_fields_to_check}
                        })
                        self.stats['failed_rows'] += 1
                        continue
                else:
                    # This product HAS VARIANTS
                    # CRITICAL: Parent product MUST NOT have ANY variant-specific fields populated
                    # If parent has variant fields, Shopify creates a "Default Title" variant with $0.00
                    # Clear ALL variant-related fields from parent (both template names and Shopify names)
                    variant_fields_template = ['Price', 'Compare-at price', 'SKU', 'Variant SKU', 'Variant Price', 
                                             'Variant Compare At Price', 'Variant Inventory Qty', 'Variant Inventory Tracker',
                                             'Variant Inventory Policy', 'Variant Fulfillment Service', 'Variant Grams',
                                             'Variant Requires Shipping', 'Variant Taxable', 'Variant Image',
                                             'Inventory quantity', 'Inventory tracker', 'Fulfillment service',
                                             'Continue selling when out of stock']
                    for field in variant_fields_template:
                        if field in parent_shopify_row:
                            parent_shopify_row[field] = ""
                    
                    # Set Option1 Name if we have variants (but don't set Option1 Value for parent)
                    # Option1 Name tells Shopify what the variant option is (e.g., "Size", "Color")
                    if 'Option1 name' in parent_shopify_row:
                        parent_shopify_row['Option1 name'] = 'Size'  # Default, can be improved
                    # CRITICAL: Parent should NOT have Option1 Value (that's only for variants)
                    # If parent has Option1 Value, Shopify creates a "Default" variant
                    # Explicitly set to empty string to avoid NaN issues
                    for col in shopify_columns:
                        if col == 'Option1 value' or col == 'Option1 Value':
                            parent_shopify_row[col] = ""
                    # Also check transformed_parent in case it was set there
                    if 'Option1 value' in transformed_parent:
                        parent_shopify_row['Option1 value'] = ""
                    if 'Option1 Value' in transformed_parent:
                        parent_shopify_row['Option1 Value'] = ""
                
                shopify_rows.append(parent_shopify_row)
                self.stats['successful_rows'] += 1
                
                # Create additional rows for remaining images (Shopify requires separate rows for each image)
                if parent_images:
                    for img_idx in range(1, len(parent_images)):
                        image_row = parent_shopify_row.copy()
                        # Clear Title for additional image rows (only first row has Title)
                        if 'Title' in image_row:
                            image_row['Title'] = ""
                        # Set this image URL
                        if 'Product image URL' in image_row:
                            image_row['Product image URL'] = parent_images[img_idx]
                        shopify_rows.append(image_row)
                        self.stats['successful_rows'] += 1
                
                # Process variants
                # Get parent images to copy to variants if they don't have one
                parent_image_first = parent_images[0] if parent_images else ""
                
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
                    
                    # Handle variant images - parse multiple images
                    variant_image_str = transformed_variant.get('Product image URL', '')
                    variant_images = []
                    if variant_image_str and pd.notna(variant_image_str) and str(variant_image_str).strip() != '':
                        # Split by comma and clean up
                        image_urls = [url.strip() for url in str(variant_image_str).split(',') if url.strip()]
                        variant_images = [url for url in image_urls if url and url != 'nan']
                    
                    # CRITICAL: Variants must have images (either their own or parent's)
                    if not variant_images:
                        if parent_image_first:
                            variant_images = [parent_image_first]
                        else:
                            # Skip variant without image (parent also has no image)
                            logger.warning(f"Skipping variant {variant_row.get('SKU', '')} - no image (parent also has no image)")
                            continue
                    
                    # Extract variant option value (try from name first, then SKU)
                    variant_option = self._extract_variant_option(
                        str(variant_row.get('Name', '')),
                        base_name,
                        sku=str(variant_row.get('SKU', ''))
                    )
                    
                    # If variant_option is empty, try extracting from SKU directly
                    if not variant_option or variant_option.strip() == '':
                        variant_sku = variant_row.get('SKU', '')
                        if variant_sku and pd.notna(variant_sku):
                            sku_str = str(variant_sku).strip()
                            # If SKU is a simple size name like "Small", "Medium", use it directly
                            sizes_map = {
                                'small': 'Small', 'medium': 'Medium', 'large': 'Large',
                                'xlarge': 'XLarge', 'xsmall': 'XSmall', 'xl': 'XL',
                                'xxl': 'XXL', 's': 'S', 'm': 'M', 'l': 'L', 'xs': 'XS', 'xxs': 'XXS'
                            }
                            if sku_str.lower() in sizes_map:
                                variant_option = sizes_map[sku_str.lower()]
                            else:
                                # Use SKU as-is if it's a reasonable variant identifier
                                variant_option = sku_str
                    
                    # If still empty, extract directly from name difference
                    if not variant_option or variant_option.strip() == '':
                        variant_name = str(variant_row.get('Name', '')).strip()
                        if variant_name.startswith(base_name):
                            potential_option = variant_name[len(base_name):].strip().lstrip('-').strip()
                            if potential_option:
                                variant_option = potential_option
                    
                    # Create variant row with proper title
                    # Variant title should be: "Product Name - Variant Option" or just variant option
                    variant_title = ""
                    if variant_option:
                        # Create variant title: "Base Name - Option" (e.g., "Product Name - Small")
                        variant_title = f"{base_name} - {variant_option}"
                    else:
                        # If no option extracted, use variant's original name
                        variant_original_name = str(variant_row.get('Name', '')).strip()
                        if variant_original_name and variant_original_name != base_name:
                            variant_title = variant_original_name
                            # Try to extract option from the name difference
                            if variant_original_name.startswith(base_name):
                                potential_option = variant_original_name[len(base_name):].strip().lstrip('-').strip()
                                if potential_option:
                                    variant_option = potential_option
                        else:
                            # Fallback: use base name with variant indicator
                            variant_title = base_name
                    
                    variant_shopify_row = {}
                    # Find Option1 columns first
                    option1_value_col = None
                    option1_name_col = None
                    for col in shopify_columns:
                        col_lower = col.lower()
                        if 'option1' in col_lower:
                            if 'value' in col_lower:
                                option1_value_col = col
                            elif 'name' in col_lower:
                                option1_name_col = col
                    
                    # Build variant row
                    # CRITICAL: In Shopify CSV format, variant rows should have EMPTY Title field
                    # Only the parent row should have a Title
                    # Variant rows are identified by: same Handle + Option1 Value set
                    for col in shopify_columns:
                        if col == 'Title':
                            # CRITICAL: Variant rows MUST have empty Title (not "Product Name - Variant")
                            # Shopify uses Handle + Option1 Value to identify variants
                            variant_shopify_row[col] = ""  # Empty Title for variants
                        elif col == 'URL handle':
                            variant_shopify_row[col] = handle  # Same handle as parent
                        elif col == option1_value_col:
                            # Set Option1 Value using the exact column name
                            variant_shopify_row[col] = variant_option if variant_option else ""
                        elif col == option1_name_col:
                            # Set Option1 Name using the exact column name (same as parent)
                            variant_shopify_row[col] = 'Size' if variant_option else ""
                        elif col in transformed_variant:
                            variant_shopify_row[col] = transformed_variant[col]
                        else:
                            variant_shopify_row[col] = ""
                    
                    # CRITICAL: Force set Option1 values (ensure they're always set)
                    if option1_value_col:
                        # Always set Option1 Value - convert to string to avoid NaN issues
                        variant_shopify_row[option1_value_col] = str(variant_option) if variant_option else ""
                    if option1_name_col:
                        # Always set Option1 Name if we have a variant option
                        variant_shopify_row[option1_name_col] = 'Size' if variant_option else ""
                    
                    # Verify values are set
                    if option1_value_col and option1_value_col in variant_shopify_row:
                        if not variant_shopify_row[option1_value_col] or str(variant_shopify_row[option1_value_col]).strip() == '':
                            # If still empty, try one more time with direct extraction
                            variant_name = str(variant_row.get('Name', '')).strip()
                            if variant_name.startswith(base_name):
                                direct_option = variant_name[len(base_name):].strip().lstrip('-').strip()
                                if direct_option:
                                    variant_shopify_row[option1_value_col] = direct_option
                                    variant_option = direct_option  # Update for title too
                    
                    # CRITICAL: Assign variant-specific images
                    # Set first variant image for the variant row
                    if variant_images:
                        variant_first_image = variant_images[0]
                        # Set both Variant Image URL and Product Image URL to variant's first image
                        for img_col in ['Variant image URL', 'Product image URL']:
                            if img_col in variant_shopify_row:
                                variant_shopify_row[img_col] = variant_first_image
                    elif parent_image_first:
                        # Use parent's first image if variant has no images
                        for img_col in ['Variant image URL', 'Product image URL']:
                            if img_col in variant_shopify_row:
                                variant_shopify_row[img_col] = parent_image_first
                    
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
                        # Use actual Stock value from source data
                        inventory_value = "100"  # Default when in stock
                        
                        if pd.notna(source_stock) and str(source_stock).strip() != '' and str(source_stock).strip() != 'nan':
                            try:
                                stock_val = float(source_stock)
                                # Use actual stock value, but if it's 0, set to 10 as default
                                if stock_val == 0:
                                    inventory_value = "10"
                                else:
                                    inventory_value = str(int(stock_val))
                            except:
                                inventory_value = str(source_stock).strip()
                                # If the string value is "0", set to 10
                                if inventory_value == "0":
                                    inventory_value = "10"
                        else:
                            # Stock is nan or empty - check In stock? field
                            # If In stock? is 1, set to 100. If 0, set to 10 (default minimum).
                            if in_stock_val == 1:
                                inventory_value = "100"
                            else:
                                inventory_value = "10"  # Changed from "0" to "10" as default minimum
                        
                        variant_shopify_row[inventory_col] = inventory_value
                        
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
                        
                        # DOUBLE CHECK: Ensure inventory tracker is "shopify" if inventory quantity is set
                        # Check all possible tracker column names
                        for col in shopify_columns:
                            if 'Inventory tracker' in col or ('Inventory' in col and 'tracker' in col.lower()):
                                if col in variant_shopify_row:
                                    if inventory_value and str(inventory_value).strip() != '' and str(inventory_value).strip() != '0':
                                        variant_shopify_row[col] = "shopify"
                                    break
                    
                    # CRITICAL: Ensure variant has valid price sourced directly from the original data
                    variant_price = self._extract_source_price(variant_row)
                    if not variant_price:
                        logger.warning(f"Skipping variant with zero/invalid price: Handle={handle}, SKU={variant_row.get('SKU', '')}, Name={str(variant_row.get('Name', ''))[:50]}")
                        continue  # Skip variants without valid price
                    try:
                        variant_price_value = float(variant_price)
                        if variant_price_value <= 0:
                            logger.warning(f"Skipping variant with zero price after parsing: Handle={handle}, SKU={variant_row.get('SKU', '')}")
                            continue
                    except Exception as e:
                        logger.warning(f"Skipping variant with unparsable price '{variant_price}': Handle={handle}, error={e}")
                        continue
                    
                    # Set price in Variant Price (and Price if necessary)
                    for price_field in ['Price', 'Variant Price']:
                        if price_field in variant_shopify_row:
                            variant_shopify_row[price_field] = variant_price
                    
                    shopify_rows.append(variant_shopify_row)
                    self.stats['successful_rows'] += 1
                    
                    # Create additional rows for remaining variant images (if variant has multiple images)
                    for img_idx in range(1, len(variant_images)):
                        variant_image_row = variant_shopify_row.copy()
                        # Set this image URL
                        if 'Product image URL' in variant_image_row:
                            variant_image_row['Product image URL'] = variant_images[img_idx]
                        # Also set Variant Image URL if it exists
                        if 'Variant image URL' in variant_image_row:
                            variant_image_row['Variant image URL'] = variant_images[img_idx]
                        shopify_rows.append(variant_image_row)
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
        
        # CRITICAL: Remove any single products with zero prices that slipped through
        # This is a final safety check - do it AFTER all rows are collected
        # First, build a map of handles to see which products have variants
        handle_to_has_variants = {}
        for row in shopify_rows:
            handle = row.get('URL handle', '') or row.get('Handle', '')
            title = row.get('Title', '')
            if handle:
                if handle not in handle_to_has_variants:
                    handle_to_has_variants[handle] = False
                # If this row has no title, it's a variant row
                if not title or pd.isna(title) or str(title).strip() == '':
                    # Check if it's actually a variant (has SKU and Option1 Value)
                    sku = row.get('Variant SKU', '') or row.get('SKU', '')
                    option1 = row.get('Option1 Value', '') or row.get('Option1 value', '')
                    if sku and pd.notna(sku) and str(sku).strip() != '' and option1 and pd.notna(option1) and str(option1).strip() != '':
                        handle_to_has_variants[handle] = True
        
        # Now filter out single products with zero prices
        filtered_shopify_rows = []
        removed_count = 0
        for row in shopify_rows:
            title = row.get('Title', '')
            if title and pd.notna(title) and str(title).strip() != '':
                # This is a parent row - check if it has variants
                handle = row.get('URL handle', '') or row.get('Handle', '')
                has_variants = handle_to_has_variants.get(handle, False) if handle else False
                
                # If no variants, it's a single product - must have valid price
                if not has_variants:
                    price = None
                    for price_field in ['Price', 'Variant Price', 'Regular price']:
                        price_val = row.get(price_field, '')
                        if price_val and pd.notna(price_val) and str(price_val).strip() != '' and str(price_val).strip().lower() != 'nan':
                            try:
                                price_float = float(str(price_val).replace('₹', '').replace(',', '').replace('$', '').strip())
                                if price_float > 0:
                                    price = price_float
                                    break
                            except:
                                pass
                    
                    if not price or price <= 0:
                        logger.warning(f"Removing single product '{title}' (handle: {handle}) - zero price detected in final filter")
                        removed_count += 1
                        continue  # Skip this row and all its image rows
            
            filtered_shopify_rows.append(row)
        
        if removed_count > 0:
            logger.warning(f"Removed {removed_count} single products with zero prices in final filter")
            shopify_rows = filtered_shopify_rows
        
        # Create output DataFrame
        if shopify_rows:
            # Ensure all rows have all columns (fill missing with empty string to avoid NaN)
            for row in shopify_rows:
                for col in shopify_columns:
                    if col not in row:
                        row[col] = ""
                    # Convert None to empty string
                    if row[col] is None:
                        row[col] = ""
            
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
            
            # CRITICAL FIX: After column mapping, ensure parent rows WITH VARIANTS have ALL variant fields EMPTY
            # This prevents Shopify from creating "Default Title" variant with $0.00
            # Single products (no variants) MUST keep variant fields populated
            variant_field_names = [
                'Variant SKU', 'Variant Price', 'Variant Compare At Price', 
                'Variant Inventory Qty', 'Variant Inventory Tracker', 'Variant Inventory Policy',
                'Variant Fulfillment Service', 'Variant Grams', 'Variant Requires Shipping',
                'Variant Taxable', 'Variant Image', 'Variant Barcode',
                'Option1 Value', 'Option2 Value', 'Option3 Value'
                # Note: Option1 Name should be SET for parent with variants (e.g., "Size")
                # But Option1 Value MUST be empty for parent
            ]
            
            for idx, row in shopify_df.iterrows():
                # Check if this is a parent row (has Title)
                if pd.notna(row.get('Title')) and str(row.get('Title', '')).strip() != '':
                    # Check if this parent has variants (if no variants, it's a single product and should keep variant fields)
                    handle = row.get('Handle', '')
                    if handle:
                        # Check if there are variant rows for this handle
                        # Variant rows are those with same Handle but different Title (or empty Title)
                        variant_rows = shopify_df[(shopify_df['Handle'] == handle) & 
                                                  (shopify_df.index != idx) &  # Exclude current row
                                                  ((shopify_df['Title'].isna()) | (shopify_df['Title'] == '') | 
                                                   (shopify_df['Title'] != row.get('Title')))]
                        
                        # Only clear variant fields if this parent HAS variants
                        if len(variant_rows) > 0:
                            # CRITICAL: Clear ALL variant-specific fields from parent WITH VARIANTS
                            # If ANY variant field is populated, Shopify creates a "Default" variant
                            for field in variant_field_names:
                                if field in shopify_df.columns:
                                    shopify_df.at[idx, field] = ""
                            
                            # CRITICAL: Ensure Option1 Value is empty for parent (only variants should have Option1 Value)
                            if 'Option1 Value' in shopify_df.columns:
                                shopify_df.at[idx, 'Option1 Value'] = ""
                            if 'Option2 Value' in shopify_df.columns:
                                shopify_df.at[idx, 'Option2 Value'] = ""
                            if 'Option3 Value' in shopify_df.columns:
                                shopify_df.at[idx, 'Option3 Value'] = ""
                            
                            # Ensure Option1 Name is set (tells Shopify what the variant option is)
                            if 'Option1 Name' in shopify_df.columns:
                                option1_name = shopify_df.at[idx, 'Option1 Name']
                                if pd.isna(option1_name) or str(option1_name).strip() == '':
                                    shopify_df.at[idx, 'Option1 Name'] = 'Size'  # Default option name
            
            # Replace any remaining NaN values with empty strings before writing
            shopify_df = shopify_df.fillna('')
            shopify_df = shopify_df.replace('nan', '')
            
            # CRITICAL FIXES: Ensure all data quality issues are resolved
            
            # 1. CRITICAL FIX: Ensure all rows with inventory quantity have inventory tracker set to "shopify"
            if 'Variant Inventory Qty' in shopify_df.columns and 'Variant Inventory Tracker' in shopify_df.columns:
                rows_fixed = 0
                for idx, row in shopify_df.iterrows():
                    inventory_qty = row.get('Variant Inventory Qty', '')
                    inventory_tracker = row.get('Variant Inventory Tracker', '')
                    
                    # If inventory quantity is set (not empty and not zero), ensure tracker is "shopify"
                    if inventory_qty and str(inventory_qty).strip() != '' and str(inventory_qty).strip() != 'nan' and str(inventory_qty).strip() != '0':
                        try:
                            qty_val = float(str(inventory_qty).strip())
                            if qty_val > 0:
                                # Inventory is tracked, so tracker must be "shopify"
                                if not inventory_tracker or str(inventory_tracker).strip().lower() != 'shopify':
                                    shopify_df.at[idx, 'Variant Inventory Tracker'] = "shopify"
                                    rows_fixed += 1
                        except:
                            # If we can't parse, but it's not empty, set to shopify
                            if str(inventory_qty).strip() != '' and str(inventory_qty).strip() != 'nan':
                                shopify_df.at[idx, 'Variant Inventory Tracker'] = "shopify"
                                rows_fixed += 1
                
                if rows_fixed > 0:
                    logger.info(f"Fixed {rows_fixed} rows with inventory tracker set to 'shopify'")
            
            # 2. Ensure variants have parent images if they don't have their own
            if 'Variant Image' in shopify_df.columns and 'Image Src' in shopify_df.columns:
                # Group by Handle to find parent-child relationships
                for handle in shopify_df['Handle'].unique():
                    handle_rows = shopify_df[shopify_df['Handle'] == handle]
                    parent_row = handle_rows[handle_rows['Title'].notna() & (handle_rows['Title'] != '')]
                    variant_rows = handle_rows[handle_rows['Title'].isna() | (handle_rows['Title'] == '')]
                    
                    if len(parent_row) > 0 and len(variant_rows) > 0:
                        parent_image = parent_row.iloc[0].get('Image Src', '')
                        if parent_image and str(parent_image).strip() != '' and str(parent_image).strip() != 'nan':
                            # Update variants that don't have Variant Image
                            for idx in variant_rows.index:
                                variant_image = shopify_df.at[idx, 'Variant Image']
                                if not variant_image or str(variant_image).strip() == '' or str(variant_image).strip() == 'nan':
                                    shopify_df.at[idx, 'Variant Image'] = str(parent_image).strip()
            
            # 3. CRITICAL FIX: Remove rows with zero prices (both variants AND single products)
            if 'Variant Price' in shopify_df.columns:
                rows_to_remove = []
                for idx, row in shopify_df.iterrows():
                        variant_price = row.get('Variant Price', '')
                        if variant_price and pd.notna(variant_price):
                            try:
                                price_val = float(str(variant_price).replace('₹', '').replace('$', '').replace(',', '').strip())
                                if price_val <= 0:
                                    # Zero or negative price found - remove this row (both variants and single products)
                                    is_variant = pd.isna(row.get('Title')) or str(row.get('Title', '')).strip() == ''
                                    row_type = 'variant' if is_variant else 'single product'
                                    logger.warning(f"Removing {row_type} with zero/negative price: Handle={row.get('Handle', '')}, Title={str(row.get('Title', ''))[:50]}, SKU={row.get('Variant SKU', '')}, Price={price_val}")
                                    rows_to_remove.append(idx)
                            except:
                                # If price can't be parsed, check if it's empty
                                if not variant_price or str(variant_price).strip() == '' or str(variant_price).strip() == 'nan':
                                    is_variant = pd.isna(row.get('Title')) or str(row.get('Title', '')).strip() == ''
                                    row_type = 'variant' if is_variant else 'single product'
                                    logger.warning(f"Removing {row_type} with empty/invalid price: Handle={row.get('Handle', '')}, Title={str(row.get('Title', ''))[:50]}, SKU={row.get('Variant SKU', '')}")
                                    rows_to_remove.append(idx)
                
                if rows_to_remove:
                    shopify_df = shopify_df.drop(rows_to_remove)
                    logger.info(f"Removed {len(rows_to_remove)} rows (variants + single products) with zero/invalid prices")
                    self.stats['failed_rows'] += len(rows_to_remove)
            
            # 4. FINAL FIX: Ensure variant Option1 Value and Name are set correctly
            # Variant rows (empty Title) MUST have Option1 Value set
            if 'Option1 Value' in shopify_df.columns and 'Title' in shopify_df.columns:
                for idx, row in shopify_df.iterrows():
                    title = row.get('Title', '')
                    option1_val = row.get('Option1 Value', '')
                    handle = row.get('Handle', '')
                    
                    # Check if this is a variant row (empty Title)
                    is_variant_row = pd.isna(title) or str(title).strip() == ''
                    
                    if is_variant_row:
                        # This is a variant row - it MUST have Option1 Value
                        if pd.isna(option1_val) or str(option1_val).strip() == '' or str(option1_val).strip().lower() == 'nan':
                            # Try to extract from Variant SKU or generate a default
                            variant_sku = row.get('Variant SKU', '')
                            if variant_sku and pd.notna(variant_sku) and str(variant_sku).strip() != '':
                                # Use SKU as Option1 Value if available
                                shopify_df.at[idx, 'Option1 Value'] = str(variant_sku).strip()
                            else:
                                # Generate a default value based on row index
                                shopify_df.at[idx, 'Option1 Value'] = f"Variant-{idx}"
                            
                            # Also set Option1 Name if not set
                            if 'Option1 Name' in shopify_df.columns:
                                option1_name = row.get('Option1 Name', '')
                                if pd.isna(option1_name) or str(option1_name).strip() == '' or str(option1_name).strip().lower() == 'nan':
                                    shopify_df.at[idx, 'Option1 Name'] = 'Size'
            
            # 5. FINAL DOUBLE CHECK: Ensure inventory tracker is "shopify" for ALL rows with inventory
            # This includes both single products and variants
            if 'Variant Inventory Qty' in shopify_df.columns and 'Variant Inventory Tracker' in shopify_df.columns:
                final_fixes = 0
                for idx, row in shopify_df.iterrows():
                    inventory_qty = row.get('Variant Inventory Qty', '')
                    inventory_tracker = row.get('Variant Inventory Tracker', '')
                    
                    # Check if inventory quantity is set (not empty and not zero)
                    if inventory_qty and str(inventory_qty).strip() != '' and str(inventory_qty).strip() != 'nan' and str(inventory_qty).strip() != '0':
                        try:
                            qty_val = float(str(inventory_qty).strip())
                            if qty_val > 0:
                                # Inventory is set, so tracker must be "shopify"
                                if not inventory_tracker or str(inventory_tracker).strip().lower() != 'shopify':
                                    shopify_df.at[idx, 'Variant Inventory Tracker'] = "shopify"
                                    final_fixes += 1
                        except:
                            # If we can't parse but it's not empty, set to shopify
                            if str(inventory_qty).strip() != '' and str(inventory_qty).strip() != 'nan':
                                shopify_df.at[idx, 'Variant Inventory Tracker'] = "shopify"
                                final_fixes += 1
                
                if final_fixes > 0:
                    logger.info(f"Final check: Fixed {final_fixes} more rows (including single products) with inventory tracker")
            
            # 6. CRITICAL: Ensure single products (with Title) have price set
            if 'Variant Price' in shopify_df.columns:
                single_products_no_price = []
                for idx, row in shopify_df.iterrows():
                    # Check single products (rows with Title)
                    if pd.notna(row.get('Title')) and str(row.get('Title', '')).strip() != '':
                        # Check if this product has variants (if no variants, it should have Variant Price)
                        handle = row.get('Handle', '')
                        if handle:
                            # Check if there are variant rows for this handle
                            variant_rows = shopify_df[(shopify_df['Handle'] == handle) & 
                                                      (shopify_df['Title'].isna() | (shopify_df['Title'] == ''))]
                            
                            # If no variants, this single product MUST have Variant Price
                            if len(variant_rows) == 0:
                                variant_price = row.get('Variant Price', '')
                                if not variant_price or pd.isna(variant_price) or str(variant_price).strip() == '' or str(variant_price).strip() == 'nan':
                                    try:
                                        # Try to get price from other fields
                                        price = row.get('Price', '') or row.get('Regular price', '')
                                        if price and pd.notna(price) and str(price).strip() != '':
                                            price_val = float(str(price).replace('₹', '').replace(',', '').strip())
                                            if price_val > 0:
                                                shopify_df.at[idx, 'Variant Price'] = price
                                                logger.info(f"Fixed single product missing Variant Price: Handle={handle}")
                                    except:
                                        single_products_no_price.append(handle)
                
                if single_products_no_price:
                    logger.warning(f"Found {len(single_products_no_price)} single products without price: {single_products_no_price[:5]}...")
            
            # 7. FINAL DOUBLE CHECK: Ensure no zero prices remain (both variants AND single products)
            if 'Variant Price' in shopify_df.columns:
                zero_price_count = 0
                rows_to_remove_final = []
                for idx, row in shopify_df.iterrows():
                    variant_price = row.get('Variant Price', '')
                    if variant_price and pd.notna(variant_price):
                        try:
                            price_val = float(str(variant_price).replace('₹', '').replace('$', '').replace(',', '').strip())
                            if price_val <= 0:
                                zero_price_count += 1
                                is_variant = pd.isna(row.get('Title')) or str(row.get('Title', '')).strip() == ''
                                row_type = 'variant' if is_variant else 'single product'
                                logger.error(f"CRITICAL: {row_type} still has zero price after all fixes: Handle={row.get('Handle', '')}, Title={str(row.get('Title', ''))[:50]}, SKU={row.get('Variant SKU', '')}")
                                rows_to_remove_final.append(idx)
                        except:
                            pass
                
                if rows_to_remove_final:
                    shopify_df = shopify_df.drop(rows_to_remove_final)
                    logger.info(f"FINAL CLEANUP: Removed {len(rows_to_remove_final)} rows with zero prices")
                    self.stats['failed_rows'] += len(rows_to_remove_final)
                
                if zero_price_count > 0:
                    logger.error(f"CRITICAL: {zero_price_count} rows still had zero prices - they have been removed!")
            
            # 8. FINAL CHECK: Ensure ALL products have descriptions with proper HTML format
            desc_col = None
            for col in ['Description', 'Body (HTML)']:
                if col in shopify_df.columns:
                    desc_col = col
                    break
            
            if desc_col:
                missing_desc_count = 0
                wrapped_desc_count = 0
                for idx, row in shopify_df.iterrows():
                    # Only check parent rows (rows with Title)
                    if pd.notna(row.get('Title')) and str(row.get('Title', '')).strip() != '':
                        product_name = str(row.get('Title', 'Product')).strip()
                        desc = row.get(desc_col, '')
                        if not desc or pd.isna(desc) or str(desc).strip() == '' or str(desc).strip().lower() == 'nan':
                            # Missing description - leave it empty (no default text)
                            shopify_df.at[idx, desc_col] = ""
                            missing_desc_count += 1
                            logger.debug(f"Product '{product_name}' has no description - leaving empty")
                        else:
                            # Check if description has proper simple format (no tabs)
                            desc_str = str(desc).strip()
                            if 'tabs-container' not in desc_str and 'tab-content' not in desc_str:
                                # Description exists but doesn't have simple format - format it
                                wrapped_desc = self.transformer._wrap_description_in_tabs(desc_str)
                                shopify_df.at[idx, desc_col] = wrapped_desc
                                wrapped_desc_count += 1
                                logger.info(f"Formatted description for product '{product_name}' in simple format")
                
                if missing_desc_count > 0:
                    logger.info(f"Found {missing_desc_count} products with no description (left empty)")
                if wrapped_desc_count > 0:
                    logger.info(f"Formatted {wrapped_desc_count} descriptions in simple format")
            
            # 8. CRITICAL FIX: Ensure fulfillment service is always set for ALL ROWS that need it (Shopify requirement)
            # - Single products (has Title, no variants) MUST have fulfillment service set
            # - Variant rows (same Handle as parent but not the parent) MUST have fulfillment service set
            # - Parent rows WITH variants should have empty variant fields (don't set fulfillment service)
            if 'Variant Fulfillment Service' in shopify_df.columns:
                fulfillment_fixes = 0
                for idx, row in shopify_df.iterrows():
                    title = row.get('Title', '')
                    handle = row.get('Handle', '')
                    
                    if not handle or pd.isna(handle):
                        continue
                    
                    # Find all rows with the same handle
                    handle_rows = shopify_df[shopify_df['Handle'] == handle]
                    
                    # Identify parent row (first row with Title, or first row if no Title rows)
                    parent_rows = handle_rows[pd.notna(handle_rows['Title']) & (handle_rows['Title'] != '')]
                    if len(parent_rows) > 0:
                        parent_idx = parent_rows.index[0]
                        # Variant rows are all other rows with the same handle
                        is_variant_row = (idx != parent_idx)
                        is_single_product = (len(handle_rows) == 1)
                    else:
                        # No parent row found, treat as single product
                        is_variant_row = False
                        is_single_product = True
                    
                    # Set fulfillment service for single products and variant rows (NOT parent rows with variants)
                    if is_single_product or is_variant_row:
                        fulfillment = row.get('Variant Fulfillment Service', '')
                        # Check if fulfillment is empty or invalid
                        if (pd.isna(fulfillment) or 
                            str(fulfillment).strip() == '' or 
                            str(fulfillment).strip().lower() == 'nan' or
                            str(fulfillment).strip().lower() != 'manual'):
                            shopify_df.at[idx, 'Variant Fulfillment Service'] = 'manual'
                            fulfillment_fixes += 1
                
                if fulfillment_fixes > 0:
                    logger.info(f"Fixed {fulfillment_fixes} rows (single products + variants) with fulfillment service set to 'manual'")
            
            # 9. CRITICAL FIX: Ensure inventory policy values match Shopify's accepted values for ALL ROWS that have variant fields populated
            # Shopify accepts ONLY "deny" or "continue" (lowercase, exact match)
            # - If Variant Price, Variant SKU, or Variant Inventory Qty is set, inventory policy MUST be set
            # - Parent rows WITH variants should have empty variant fields (no inventory policy needed)
            if 'Variant Inventory Policy' in shopify_df.columns:
                policy_fixes = 0
                for idx, row in shopify_df.iterrows():
                    # Check if this row has any variant fields populated
                    variant_price = row.get('Variant Price', '')
                    variant_sku = row.get('Variant SKU', '')
                    variant_inventory = row.get('Variant Inventory Qty', '')
                    
                    # If any variant field is populated, inventory policy must be set
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
                            # Map invalid/empty values to valid ones
                            if policy_str in ['true', '1', 'yes', 'y', 'allow', 'allow_continue', 'continue']:
                                shopify_df.at[idx, 'Variant Inventory Policy'] = 'continue'
                            else:
                                shopify_df.at[idx, 'Variant Inventory Policy'] = 'deny'
                            policy_fixes += 1
                
                if policy_fixes > 0:
                    logger.info(f"Fixed {policy_fixes} rows with variant fields populated - set inventory policy to valid values ('deny' or 'continue')")
            
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

