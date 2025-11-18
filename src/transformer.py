"""
Data Transformer Module
Transforms mapped data to match Shopify's specific format requirements.
"""

import re
from typing import Dict, Any, List, Optional
from loguru import logger
import pandas as pd


class DataTransformer:
    """Transform data to Shopify format requirements."""
    
    def __init__(self):
        """Initialize data transformer."""
        pass
    
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
        
        # Handle fulfillment service based on inventory tracker
        # Shopify rule: 
        # - If inventory tracker is "shopify", fulfillment service should be "manual"
        # - If inventory tracker is "not tracked", fulfillment service must be empty
        # Default: Always use "shopify" + "manual" for compatibility
        if 'Fulfillment service' in transformed:
            inventory_tracker = transformed.get('Inventory tracker', 'shopify')
            if inventory_tracker == 'not tracked' or inventory_tracker == '' or str(inventory_tracker).lower() == 'not tracked':
                # When inventory is not tracked, fulfillment service must be empty
                transformed['Fulfillment service'] = ''
            else:
                # Default: If inventory is tracked (shopify), set to 'manual'
                transformed['Fulfillment service'] = 'manual'
        
        # Ensure fulfillment service is always set (default to 'manual' if empty)
        if 'Fulfillment service' in transformed:
            if not transformed.get('Fulfillment service') or transformed.get('Fulfillment service') == '':
                # If empty, default to 'manual' and set inventory tracker to 'shopify'
                transformed['Fulfillment service'] = 'manual'
                transformed['Inventory tracker'] = 'shopify'
        
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
        
        Args:
            value: Inventory policy value
            
        Returns:
            "deny" or "continue"
        """
        if pd.isna(value) or value is None:
            return "deny"
        
        value_str = str(value).strip().lower()
        
        if value_str in ['true', '1', 'yes', 'y', 'continue', 'allow']:
            return "continue"
        elif value_str in ['false', '0', 'no', 'n', 'deny', 'deny']:
            return "deny"
        else:
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
    
    def _transform_html(self, value: Any) -> str:
        """
        Transform HTML content (clean and validate).
        Converts literal \n to HTML line breaks and fixes formatting.
        
        Args:
            value: HTML content
            
        Returns:
            Cleaned HTML string
        """
        if pd.isna(value):
            return ""
        
        html = str(value).strip()
        
        # Convert literal \n (escaped newlines) to actual newlines
        html = html.replace('\\n', '\n')
        
        # If the content already has HTML tags, preserve them but clean up newlines
        if '<' in html and '>' in html:
            # Has HTML tags - convert newlines between tags to proper spacing
            # Replace newlines that are not inside tags with <br>
            # Split by newlines and process each line
            lines = html.split('\n')
            cleaned_lines = []
            for line in lines:
                line = line.strip()
                if line:
                    # If line is already an HTML tag, keep it as is
                    if line.startswith('<') and line.endswith('>'):
                        cleaned_lines.append(line)
                    # If line contains HTML tags, preserve them
                    elif '<' in line and '>' in line:
                        cleaned_lines.append(line)
                    # Otherwise, wrap in paragraph or add <br>
                    else:
                        if cleaned_lines and not cleaned_lines[-1].endswith('>'):
                            cleaned_lines.append('<br>')
                        cleaned_lines.append(line)
            
            html = '\n'.join(cleaned_lines)
            # Replace remaining standalone newlines with <br>
            html = re.sub(r'\n(?!<)', '<br>\n', html)
        else:
            # Plain text - convert to proper HTML paragraphs
            paragraphs = [p.strip() for p in html.split('\n') if p.strip()]
            if paragraphs:
                html = '<p>' + '</p>\n<p>'.join(paragraphs) + '</p>'
            else:
                html = html.replace('\n', '<br>')
        
        # Basic HTML cleaning (remove script tags, etc.)
        html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        
        # Clean up multiple consecutive <br> tags (more than 2)
        html = re.sub(r'(<br>\s*){3,}', '<br><br>', html, flags=re.IGNORECASE)
        
        # Clean up empty paragraphs
        html = re.sub(r'<p>\s*</p>', '', html, flags=re.IGNORECASE)
        
        # Remove trailing <br> tags at the end
        html = re.sub(r'<br>\s*$', '', html, flags=re.IGNORECASE | re.MULTILINE)
        
        return html
    
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

