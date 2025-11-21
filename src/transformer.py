"""
Data Transformer Module
Transforms mapped data to match Shopify's specific format requirements.
"""

import re
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from loguru import logger
import pandas as pd


class DataTransformer:
    """Transform data to Shopify format requirements."""
    
    def __init__(self, category_mapping_path: Optional[str] = None):
        """
        Initialize data transformer.
        
        Args:
            category_mapping_path: Optional path to category mapping JSON file
        """
        self.category_mappings = {}
        self.fallback_strategy = "clear"
        self.default_category = ""
        
        if category_mapping_path:
            self.load_category_mappings(category_mapping_path)
    
    def load_category_mappings(self, mapping_path: str) -> None:
        """
        Load category mappings from JSON file.
        
        Args:
            mapping_path: Path to category mapping JSON file
        """
        try:
            mapping_file = Path(mapping_path)
            if mapping_file.exists():
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    self.category_mappings = config.get('category_mappings', {}).get('mappings', {})
                    self.fallback_strategy = config.get('category_mappings', {}).get('fallback_strategy', 'clear')
                    self.default_category = config.get('category_mappings', {}).get('default_category', '')
                    logger.info(f"Loaded {len(self.category_mappings)} category mappings from {mapping_path}")
            else:
                logger.warning(f"Category mapping file not found: {mapping_path}")
        except Exception as e:
            logger.warning(f"Could not load category mappings: {e}. Continuing without category mapping.")
    
    def transform_row(self, mapped_row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a mapped row to Shopify format.
        
        Args:
            mapped_row: Dictionary with mapped fields
            
        Returns:
            Dictionary with transformed fields
        """
        transformed = mapped_row.copy()
        
        # Transform each field based on Shopify requirements
        for field, value in transformed.items():
            if pd.isna(value) or value is None:
                continue
            
            # Handle different field types
            if 'Price' in field or 'Compare At Price' in field or 'Compare-at price' in field:
                transformed[field] = self._transform_price(value)
            elif 'Inventory' in field and 'quantity' in field.lower():
                transformed[field] = self._transform_inventory(value)
            elif 'Inventory tracker' in field:
                transformed[field] = self._transform_inventory_tracker(value)
            elif 'Image' in field:
                transformed[field] = self._transform_image_url(value)
            elif 'Tags' in field:
                transformed[field] = self._transform_tags(value)
            elif 'Handle' in field or 'handle' in field.lower():
                transformed[field] = self._transform_handle(value)
            elif 'Body' in field or 'Description' in field:
                transformed[field] = self._transform_html(value)
            elif 'Published' in field:
                transformed[field] = self._transform_boolean(value)
            elif field == 'Status':
                # Status should be 'active' or 'draft', not TRUE/FALSE
                transformed[field] = self._transform_status(value)
            elif 'Continue selling' in field or 'Inventory policy' in field:
                transformed[field] = self._transform_inventory_policy(value)
            elif 'Fulfillment service' in field:
                # Fulfillment service depends on inventory tracker
                # Will be set after inventory tracker is processed
                pass
            elif 'SEO' in field:
                transformed[field] = self._transform_seo(value)
            elif 'Product category' in field or ('category' in field.lower() and 'Product' in field):
                transformed[field] = self._transform_category(value)
        
        # CRITICAL: Ensure Product category field always exists and has a value
        # Check if Product category field exists in transformed row
        product_category_field = None
        for field in transformed.keys():
            if 'Product category' in field or ('category' in field.lower() and 'Product' in field):
                product_category_field = field
                break
        
        # If Product category field exists but is empty, apply default category
        if product_category_field:
            category_value = transformed.get(product_category_field, '')
            if not category_value or str(category_value).strip() == '':
                # Apply default category if configured
                if self.fallback_strategy == 'default' and self.default_category and str(self.default_category).strip() != '':
                    transformed[product_category_field] = str(self.default_category).strip()
        else:
            # If Product category field doesn't exist, try to find it and set default
            # This handles cases where the field wasn't in the mapped row
            for field in ['Product category', 'Product Category', 'product_category']:
                if field in transformed:
                    if self.fallback_strategy == 'default' and self.default_category and str(self.default_category).strip() != '':
                        transformed[field] = str(self.default_category).strip()
                    break
        
        # Handle fulfillment service based on inventory tracker
        # CRITICAL FIX: Shopify requires fulfillment service to be set (cannot be blank)
        # Always set to "manual" for all products/variants
        if 'Fulfillment service' in transformed:
            # Shopify requires fulfillment service to be set, so always use "manual"
                transformed['Fulfillment service'] = 'manual'
        
        # Ensure "Continue selling when out of stock" (Inventory policy) is set correctly
        if 'Continue selling when out of stock' in transformed:
            policy = transformed.get('Continue selling when out of stock', '')
            if not policy or pd.isna(policy) or str(policy).strip() == '':
                transformed['Continue selling when out of stock'] = 'deny'
        
        return transformed
    
    def _transform_price(self, value: Any) -> str:
        """
        Transform price to Shopify format (2 decimal places).
        
        Args:
            value: Price value
            
        Returns:
            Formatted price string
        """
        try:
            # Remove currency symbols and whitespace
            price_str = str(value).strip()
            price_str = re.sub(r'[^\d.]', '', price_str)
            
            # Convert to float and format
            price = float(price_str)
            if price < 0:
                logger.warning(f"Negative price found: {price}, setting to 0")
                price = 0.0
            
            return f"{price:.2f}"
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not transform price '{value}': {e}")
            return "0.00"
    
    def _transform_inventory_tracker(self, value: Any) -> str:
        """
        Transform inventory tracker to Shopify format.
        
        Args:
            value: Inventory tracker value
            
        Returns:
            "shopify" or "not tracked"
        """
        # Default to "shopify" for variants (will be overridden in migration.py if needed)
        # This ensures variants with inventory quantity are tracked by default
        if pd.isna(value) or value is None:
            return "shopify"  # Changed from "not tracked" to "shopify" as default
        
        value_str = str(value).strip().lower()
        
        # Handle numeric values
        if value_str in ['0', '0.0']:
            return "not tracked"
        elif value_str in ['1', '1.0']:
            return "shopify"
        
        # Handle string values
        if 'not tracked' in value_str:
            return "not tracked"
        elif 'shopify' in value_str or value_str == '':
            return "shopify"  # Default empty to "shopify"
        else:
            # Default to shopify if we have any value
            return "shopify"
    
    def _transform_inventory_policy(self, value: Any) -> str:
        """
        Transform inventory policy (Continue selling when out of stock) to Shopify format.
        Shopify accepts ONLY: "deny" or "continue" (lowercase, exact match required)
        
        Args:
            value: Inventory policy value
            
        Returns:
            "deny" or "continue" (Shopify-accepted values only)
        """
        if pd.isna(value) or value is None:
            return "deny"
        
        value_str = str(value).strip().lower()
        
        # Map to Shopify-accepted values only ("deny" or "continue")
        if value_str in ['true', '1', 'yes', 'y', 'continue', 'allow', 'allow_continue']:
            return "continue"
        elif value_str in ['false', '0', 'no', 'n', 'deny', 'stop', 'disallow']:
            return "deny"
        else:
            # Default to "deny" if value doesn't match expected values
            # This ensures we always return a valid Shopify value
            return "deny"
    
    def _transform_inventory(self, value: Any) -> str:
        """
        Transform inventory quantity to integer.
        
        Args:
            value: Inventory value
            
        Returns:
            Formatted inventory string
        """
        if pd.isna(value) or value is None:
            return "0"
        
        value_str = str(value).strip().lower()
        
        # Handle "not tracked" or similar strings
        if value_str in ['not tracked', 'not_tracked', 'none', 'n/a', 'na', '']:
            return "0"
        
        try:
            qty = int(float(value_str))
            if qty < 0:
                logger.warning(f"Negative inventory found: {qty}, setting to 0")
                qty = 0
            return str(qty)
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not transform inventory '{value}': {e}")
            return "0"
    
    def _transform_image_url(self, value: Any) -> str:
        """
        Transform image URL to Shopify format.
        
        Args:
            value: Image URL or comma-separated URLs
            
        Returns:
            Formatted image URL string
        """
        if pd.isna(value):
            return ""
        
        url_str = str(value).strip()
        
        # Handle comma-separated URLs
        urls = [url.strip() for url in url_str.split(',') if url.strip()]
        
        # Validate URLs (basic check)
        valid_urls = []
        for url in urls:
            if self._is_valid_url(url):
                valid_urls.append(url)
            else:
                logger.warning(f"Invalid image URL: {url}")
        
        return ','.join(valid_urls) if valid_urls else ""
    
    def _is_valid_url(self, url: str) -> bool:
        """
        Basic URL validation.
        
        Args:
            url: URL string to validate
            
        Returns:
            True if URL appears valid
        """
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        return bool(url_pattern.match(url))
    
    def _transform_tags(self, value: Any) -> str:
        """
        Transform tags to comma-separated format.
        
        Args:
            value: Tags value (can be comma-separated, semicolon-separated, or list)
            
        Returns:
            Comma-separated tags string
        """
        if pd.isna(value):
            return ""
        
        tag_str = str(value).strip()
        
        # Handle different separators
        if ';' in tag_str:
            tags = [tag.strip() for tag in tag_str.split(';')]
        elif ',' in tag_str:
            tags = [tag.strip() for tag in tag_str.split(',')]
        else:
            tags = [tag_str]
        
        # Clean tags (remove empty, trim whitespace)
        clean_tags = [tag for tag in tags if tag]
        
        return ','.join(clean_tags)
    
    def _transform_handle(self, value: Any) -> str:
        """
        Transform handle to URL-friendly format.
        
        Args:
            value: Handle value (usually from Title)
            
        Returns:
            URL-friendly handle string
        """
        if pd.isna(value):
            return ""
        
        handle = str(value).strip()
        
        # Convert to lowercase
        handle = handle.lower()
        
        # Replace spaces and special characters with hyphens
        # Keep alphanumeric and hyphens only
        handle = re.sub(r'[^\w\s-]', '', handle)
        
        # Replace multiple spaces/hyphens with single hyphen
        handle = re.sub(r'[-\s]+', '-', handle)
        
        # Remove leading/trailing hyphens
        handle = handle.strip('-')
        
        # Limit length (Shopify handles should be reasonable)
        if len(handle) > 255:
            handle = handle[:255].rstrip('-')
        
        return handle
    
    def _create_default_description(self, product_name: str = "") -> str:
        """
        Create a default description with simple heading/paragraph format when no description is available.
        
        Args:
            product_name: Name of the product
            
        Returns:
            HTML string with headings and paragraphs (NO TABS)
        """
        product_name = product_name or "Product"
        
        default_html = f"""<h4>Overview</h4>
<p>{product_name} - Product details coming soon.</p>

<h4>Specifications</h4>
<p>Specifications will be added soon.</p>

<h4>Additional Information</h4>
<p>Additional information will be added soon.</p>"""
        return default_html
    
    def _wrap_description_in_tabs(self, description_text: str) -> str:
        """
        Format existing description text with simple heading/paragraph format.
        
        Args:
            description_text: Plain text or HTML description
            
        Returns:
            HTML string with headings and paragraphs (NO TABS)
        """
        # Clean the description text
        desc_clean = str(description_text).strip()
        desc_clean = desc_clean.replace('\\n', '\n')
        
        # Use the simple structure format (no tabs)
        return self._structure_description_simple(desc_clean)
    
    def _transform_html(self, value: Any) -> str:
        """
        Transform HTML content (clean and validate).
        Converts literal \n to HTML line breaks and fixes formatting.
        Creates structured description with headings and paragraphs (NO TABS).
        PRESERVES original colors and styles from source.
        
        Args:
            value: HTML content
            
        Returns:
            Cleaned HTML string with headings and paragraphs (preserves original formatting)
        """
        if pd.isna(value):
            return ""
        
        html = str(value).strip()
        
        # Convert literal \n (escaped newlines) to actual newlines
        html = html.replace('\\n', '\n')
        
        # Basic HTML cleaning (remove script tags only - preserve everything else)
        html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        
        # DO NOT remove colors - preserve original source formatting
        
        # Structure content into simple format with headings and paragraphs (NO TABS)
        html = self._structure_description_simple(html)
        
        # Clean up multiple consecutive <br> tags (more than 2)
        html = re.sub(r'(<br>\s*){3,}', '<br><br>', html, flags=re.IGNORECASE)
        
        # Clean up empty paragraphs
        html = re.sub(r'<p>\s*</p>', '', html, flags=re.IGNORECASE)
        
        # Remove trailing <br> tags at the end
        html = re.sub(r'<br>\s*$', '', html, flags=re.IGNORECASE | re.MULTILINE)
        
        return html
    
    def _structure_description_simple(self, content: str) -> str:
        """
        Structure description content into a simple format with headings and paragraphs.
        Only uses Overview and Specifications sections - NO Additional Information.
        Uses ONLY source file content - NO placeholder text.
        Preserves original source content without modifying colors or styles.
        
        Args:
            content: Raw HTML or text content
            
        Returns:
            Structured HTML with headings and paragraphs (no tabs, only Overview and Specifications)
        """
        # Check if content has explicit specification marker from mapper
        if '---SPECIFICATIONS---' in content:
            parts = content.split('---SPECIFICATIONS---')
            overview_content_raw = parts[0].strip() if len(parts) > 0 else ''
            specs_content_raw = parts[1].strip() if len(parts) > 1 else ''
        else:
            # Split content into lines for processing
            lines = content.split('\n')
            cleaned_lines = [line.strip() for line in lines if line.strip()]
            
            # Try to detect existing sections
            overview_content_raw = []
            specs_content_raw = []
            
            current_section = 'overview'
            section_keywords = {
                'overview': ['overview', 'description', 'about', 'introduction', 'details', 'product'],
                'specifications': ['specification', 'spec', 'features', 'technical', 'dimensions', 'size', 'weight', 'material']
            }
            
            # Parse content into sections
            for line in cleaned_lines:
                line_lower = line.lower()
                # Check if line indicates a section change
                section_found = False
                for section, keywords in section_keywords.items():
                    for keyword in keywords:
                        if keyword in line_lower and len(line) < 100:  # Likely a heading
                            current_section = section
                            section_found = True
                            break
                    if section_found:
                        break
                
                # Add line to appropriate section (skip the heading line itself)
                if not section_found:
                    if current_section == 'overview':
                        overview_content_raw.append(line)
                    elif current_section == 'specifications':
                        specs_content_raw.append(line)
            
            # If no content was parsed into sections, put everything in overview
            if not overview_content_raw and not specs_content_raw:
                overview_content_raw = cleaned_lines if cleaned_lines else [content]
            
            # Convert lists to strings
            overview_content_raw = '\n'.join(overview_content_raw) if isinstance(overview_content_raw, list) else overview_content_raw
            specs_content_raw = '\n'.join(specs_content_raw) if isinstance(specs_content_raw, list) else specs_content_raw
        
        # Format content for each section (ONLY if content exists - NO placeholder text)
        structured_html = []
        
        # Overview section - ONLY if we have content
        if overview_content_raw and str(overview_content_raw).strip():
            overview_lines = overview_content_raw.split('\n') if isinstance(overview_content_raw, str) else overview_content_raw
            overview_html = self._format_section_content_simple(overview_lines)
            if overview_html and overview_html.strip() and overview_html != '<p></p>':
                structured_html.append('<h4>Overview</h4>')
                structured_html.append(overview_html)
        
        # Specifications section - ONLY if we have content
        if specs_content_raw and str(specs_content_raw).strip():
            specs_lines = specs_content_raw.split('\n') if isinstance(specs_content_raw, str) else specs_content_raw
            specs_html = self._format_section_content_simple(specs_lines)
            if specs_html and specs_html.strip() and specs_html != '<p></p>':
                structured_html.append('<h4>Specifications</h4>')
                structured_html.append(specs_html)
        
        # Return structured HTML - if no content, return empty string (NO placeholder text)
        if structured_html:
            return '\n'.join(structured_html)
        else:
            return ''  # Return empty if no content - NO placeholder text
    
    def _format_section_content_simple(self, lines: List[str]) -> str:
        """
        Format section content preserving original HTML/formatting.
        Uses simple paragraphs, preserves colors and styles from source.
        
        Args:
            lines: List of content lines
            
        Returns:
            Formatted HTML string preserving original content
        """
        if not lines:
            return '<p></p>'
        
        formatted = []
        current_paragraph = []
        
        for line in lines:
            line = line.strip()
            if not line:
                # Empty line - close current paragraph if any
                if current_paragraph:
                    para_text = ' '.join(current_paragraph)
                    # Preserve original HTML if present, otherwise wrap in <p>
                    if para_text.startswith('<') and para_text.endswith('>'):
                        formatted.append(para_text)
                    else:
                        formatted.append(f'<p>{para_text}</p>')
                    current_paragraph = []
                continue
            
            # Check if line is already HTML
            if line.startswith('<') and line.endswith('>'):
                # Close current paragraph if any
                if current_paragraph:
                    para_text = ' '.join(current_paragraph)
                    if para_text.startswith('<') and para_text.endswith('>'):
                        formatted.append(para_text)
                    else:
                        formatted.append(f'<p>{para_text}</p>')
                    current_paragraph = []
                # Add HTML line as-is (preserve original formatting)
                formatted.append(line)
            else:
                # Regular text - add to current paragraph
                current_paragraph.append(line)
        
        # Close any remaining paragraph
        if current_paragraph:
            para_text = ' '.join(current_paragraph)
            if para_text.startswith('<') and para_text.endswith('>'):
                formatted.append(para_text)
            else:
                formatted.append(f'<p>{para_text}</p>')
        
        return '\n'.join(formatted) if formatted else '<p></p>'
    
    def _format_section_content(self, lines: List[str]) -> str:
        """
        Format section content with proper headings, subheadings, and paragraphs.
        
        Args:
            lines: List of content lines
            
        Returns:
            Formatted HTML string
        """
        if not lines:
            return '<p></p>'
        
        formatted = []
        i = 0
        
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                i += 1
                continue
            
            # Check if line is likely a heading (short, all caps, or starts with number)
            is_heading = False
            is_subheading = False
            
            # Check for heading patterns
            if len(line) < 80:
                # Check if it's all caps (likely heading)
                if line.isupper() and len(line.split()) <= 8:
                    is_heading = True
                # Check if it starts with a number or bullet
                elif re.match(r'^[\d#•\-\*]', line):
                    is_subheading = True
                # Check if it's a short line that might be a heading
                elif len(line.split()) <= 5 and not line.endswith('.') and not line.endswith(','):
                    is_subheading = True
            
            # Check if line already contains HTML heading tags
            if re.match(r'<h[1-6]', line, re.IGNORECASE):
                formatted.append(line)
            elif is_heading:
                # Main heading - use smaller h5 instead of h3
                clean_line = re.sub(r'<[^>]+>', '', line)  # Remove any existing HTML
                formatted.append(f'<h5>{clean_line}</h5>')
            elif is_subheading:
                # Subheading - use h6 for smaller size
                clean_line = re.sub(r'<[^>]+>', '', line)  # Remove any existing HTML
                formatted.append(f'<h6>{clean_line}</h6>')
            else:
                # Regular paragraph
                # Check if line already has HTML tags
                if '<' in line and '>' in line:
                    # Preserve existing HTML but ensure it's in a paragraph if needed
                    if not line.startswith('<'):
                        formatted.append(f'<p>{line}</p>')
                    else:
                        formatted.append(line)
                else:
                    # Plain text - wrap in paragraph
                    formatted.append(f'<p>{line}</p>')
            
            i += 1
        
        return '\n'.join(formatted) if formatted else '<p></p>'
    
    def _transform_status(self, value: Any) -> str:
        """
        Transform status value to Shopify format (active/draft).
        
        Args:
            value: Status value
            
        Returns:
            "active" or "draft"
        """
        if pd.isna(value):
            return "draft"
        
        value_str = str(value).strip().upper()
        
        if value_str in ['TRUE', '1', 'YES', 'Y', 'ACTIVE', 'PUBLISHED']:
            return "active"
        elif value_str in ['FALSE', '0', 'NO', 'N', 'INACTIVE', 'DRAFT']:
            return "draft"
        else:
            # If it's already 'active' or 'draft', return as is
            if value_str.lower() in ['active', 'draft']:
                return value_str.lower()
            logger.warning(f"Unknown status value: {value}, defaulting to draft")
            return "draft"
    
    def _transform_boolean(self, value: Any) -> str:
        """
        Transform boolean value to Shopify format.
        For "Published on online store": "True" or "False" (capitalized)
        For "Status": "active" or "draft" (lowercase)
        
        Args:
            value: Boolean value
            
        Returns:
            "True"/"False" for published fields, "active"/"draft" for status
        """
        if pd.isna(value):
            return "False"
        
        value_str = str(value).strip().upper()
        
        if value_str in ['TRUE', '1', 'YES', 'Y', 'ACTIVE', 'PUBLISHED']:
            return "True"
        elif value_str in ['FALSE', '0', 'NO', 'N', 'INACTIVE', 'DRAFT']:
            return "False"
        else:
            logger.warning(f"Unknown boolean value: {value}, defaulting to False")
            return "False"
    
    def _transform_seo(self, value: Any) -> str:
        """
        Transform SEO field (meta title/description).
        
        Args:
            value: SEO value
            
        Returns:
            Cleaned SEO string
        """
        if pd.isna(value):
            return ""
        
        seo = str(value).strip()
        
        # Remove HTML tags
        seo = re.sub(r'<[^>]+>', '', seo)
        
        # Limit length (SEO best practices)
        if 'Title' in str(value):
            seo = seo[:70]  # Meta title should be < 70 chars
        elif 'Description' in str(value):
            seo = seo[:160]  # Meta description should be < 160 chars
        
        return seo
    
    def _transform_category(self, value: Any) -> str:
        """
        Transform category to Shopify format.
        Shopify expects: "Category > Subcategory" or just "Category"
        Source format may have multiple paths separated by commas.
        
        Maps categories to Shopify's Standard Product Taxonomy if mappings are available.
        
        Args:
            value: Category value from source
            
        Returns:
            Formatted category string for Shopify (mapped to valid taxonomy if possible)
        """
        if pd.isna(value) or not value:
            return ""
        
        category_str = str(value).strip()
        
        # If multiple categories separated by comma, take the first one
        if ',' in category_str:
            # Split by comma and take the first category path
            category_str = category_str.split(',')[0].strip()
        
        # Clean up: normalize spaces around ">"
        category_str = re.sub(r'\s*>\s*', ' > ', category_str)
        
        # Remove trailing commas and clean up extra spaces
        category_str = category_str.rstrip(',').strip()
        
        # Remove duplicate category names in the path
        # Example: "Category > Subcategory > Category" -> "Category > Subcategory"
        parts = [p.strip() for p in category_str.split('>')]
        seen = set()
        cleaned_parts = []
        for part in parts:
            part_clean = part.strip()
            if part_clean and part_clean.lower() not in seen:
                cleaned_parts.append(part_clean)
                seen.add(part_clean.lower())
        
        if cleaned_parts:
            category_str = ' > '.join(cleaned_parts)
        
        # Try to map to Shopify's valid taxonomy
        mapped_category = self._map_to_shopify_taxonomy(category_str)
        
        return mapped_category
    
    def _map_to_shopify_taxonomy(self, category: str) -> str:
        """
        Map source category to Shopify's Standard Product Taxonomy.
        
        Args:
            category: Source category string
            
        Returns:
            Mapped category string (Shopify valid taxonomy) or empty string if unmapped
        """
        if not category:
            return ""
        
        # Try exact match first
        if category in self.category_mappings:
            return self.category_mappings[category]
        
        # Try partial match (check if any part of the category path matches)
        # Check main category (first part before >)
        main_category = category.split('>')[0].strip() if '>' in category else category.strip()
        if main_category in self.category_mappings:
            return self.category_mappings[main_category]
        
        # Try case-insensitive match
        category_lower = category.lower()
        for source_cat, shopify_cat in self.category_mappings.items():
            if source_cat.lower() == category_lower:
                return shopify_cat
            # Check if source category is a prefix of the current category
            if category_lower.startswith(source_cat.lower()):
                return shopify_cat
        
        # No mapping found - apply fallback strategy
        if self.fallback_strategy == "clear":
            logger.debug(f"No Shopify taxonomy mapping found for category: '{category}'. Clearing category.")
            return ""
        elif self.fallback_strategy == "default":
            if self.default_category:
                logger.debug(f"No Shopify taxonomy mapping found for category: '{category}'. Using default category: {self.default_category}")
                return self.default_category
            else:
                logger.debug(f"No Shopify taxonomy mapping found for category: '{category}'. No default category set, clearing.")
                return ""
        elif self.fallback_strategy == "warn":
            logger.warning(f"No Shopify taxonomy mapping found for category: '{category}'. Keeping original (may cause Shopify import warning).")
            return category
        else:
            # Default to clear if unknown strategy
            logger.debug(f"Unknown fallback strategy '{self.fallback_strategy}'. Clearing category.")
            return ""

