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
            elif 'Inventory' in field or 'Qty' in field or 'quantity' in field.lower():
                transformed[field] = self._transform_inventory(value)
            elif 'Image' in field:
                transformed[field] = self._transform_image_url(value)
            elif 'Tags' in field:
                transformed[field] = self._transform_tags(value)
            elif 'Handle' in field or 'handle' in field.lower():
                transformed[field] = self._transform_handle(value)
            elif 'Body' in field or 'Description' in field:
                transformed[field] = self._transform_html(value)
            elif 'Published' in field or 'Status' in field:
                transformed[field] = self._transform_boolean(value)
            elif 'SEO' in field:
                transformed[field] = self._transform_seo(value)
        
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
        
        Args:
            value: HTML content
            
        Returns:
            Cleaned HTML string
        """
        if pd.isna(value):
            return ""
        
        html = str(value).strip()
        
        # Basic HTML cleaning (remove script tags, etc.)
        html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        
        return html
    
    def _transform_boolean(self, value: Any) -> str:
        """
        Transform boolean value to Shopify format (TRUE/FALSE).
        
        Args:
            value: Boolean value
            
        Returns:
            "TRUE" or "FALSE"
        """
        if pd.isna(value):
            return "FALSE"
        
        value_str = str(value).strip().upper()
        
        if value_str in ['TRUE', '1', 'YES', 'Y', 'ACTIVE', 'PUBLISHED']:
            return "TRUE"
        elif value_str in ['FALSE', '0', 'NO', 'N', 'INACTIVE', 'DRAFT']:
            return "FALSE"
        else:
            logger.warning(f"Unknown boolean value: {value}, defaulting to FALSE")
            return "FALSE"
    
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

