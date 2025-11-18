"""
Data Validator Module
Validates source data and transformed Shopify data.
"""

from typing import Dict, Any, List, Tuple, Optional
from loguru import logger
import pandas as pd
import re


class DataValidator:
    """Validate data before and after transformation."""
    
    def __init__(self, required_fields: Optional[List[str]] = None):
        """
        Initialize validator.
        
        Args:
            required_fields: List of required field names
        """
        self.required_fields = required_fields or []
        self.errors = []
        self.warnings = []
    
    def validate_source_row(self, row: Dict[str, Any], row_number: int) -> Tuple[bool, List[str]]:
        """
        Validate a source row before transformation.
        
        Args:
            row: Source row dictionary
            row_number: Row number for error reporting
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check for completely empty rows
        if not any(pd.notna(v) and str(v).strip() for v in row.values()):
            errors.append(f"Row {row_number}: Row is completely empty")
            return False, errors
        
        return len(errors) == 0, errors
    
    def validate_shopify_row(
        self,
        row: Dict[str, Any],
        row_number: int,
        required_fields: Optional[List[str]] = None
    ) -> Tuple[bool, List[str], List[str]]:
        """
        Validate a transformed Shopify row.
        
        Args:
            row: Shopify row dictionary
            row_number: Row number for error reporting
            required_fields: List of required fields (overrides instance default)
            
        Returns:
            Tuple of (is_valid, list_of_errors, list_of_warnings)
        """
        errors = []
        warnings = []
        
        required = required_fields or self.required_fields
        
        # Check required fields
        for field in required:
            if field not in row or pd.isna(row.get(field)) or str(row.get(field, '')).strip() == '':
                errors.append(f"Row {row_number}: Missing required field '{field}'")
        
        # Validate specific field formats
        for field, value in row.items():
            if pd.isna(value):
                continue
            
            value_str = str(value).strip()
            
            # Validate price fields
            if 'Price' in field:
                if not self._is_valid_price(value_str):
                    errors.append(f"Row {row_number}: Invalid price format in '{field}': {value_str}")
            
            # Validate inventory quantity fields (but not Inventory tracker which is a string)
            if ('Inventory' in field or 'Qty' in field) and 'tracker' not in field.lower():
                if not self._is_valid_inventory(value_str):
                    errors.append(f"Row {row_number}: Invalid inventory in '{field}': {value_str}")
            
            # Validate image URLs
            if 'Image' in field and value_str:
                invalid_urls = self._validate_image_urls(value_str)
                if invalid_urls:
                    warnings.append(f"Row {row_number}: Invalid image URLs in '{field}': {', '.join(invalid_urls)}")
            
            # Validate handle
            if 'Handle' in field:
                if not self._is_valid_handle(value_str):
                    errors.append(f"Row {row_number}: Invalid handle format in '{field}': {value_str}")
            
            # Validate boolean fields
            if 'Published' in field or 'Status' in field:
                if value_str not in ['TRUE', 'FALSE']:
                    warnings.append(f"Row {row_number}: Boolean field '{field}' should be TRUE/FALSE, got: {value_str}")
        
        return len(errors) == 0, errors, warnings
    
    def _is_valid_price(self, value: str) -> bool:
        """
        Validate price format.
        
        Args:
            value: Price string
            
        Returns:
            True if valid price format
        """
        try:
            price = float(value)
            return price >= 0 and len(value.split('.')[-1]) <= 2
        except ValueError:
            return False
    
    def _is_valid_inventory(self, value: str) -> bool:
        """
        Validate inventory format.
        
        Args:
            value: Inventory string
            
        Returns:
            True if valid inventory format
        """
        try:
            qty = int(value)
            return qty >= 0
        except ValueError:
            return False
    
    def _validate_image_urls(self, urls_str: str) -> List[str]:
        """
        Validate image URLs and return list of invalid ones.
        
        Args:
            urls_str: Comma-separated URLs
            
        Returns:
            List of invalid URLs
        """
        invalid = []
        urls = [url.strip() for url in urls_str.split(',') if url.strip()]
        
        url_pattern = re.compile(
            r'^https?://'
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'
            r'localhost|'
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
            r'(?::\d+)?'
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        for url in urls:
            if not url_pattern.match(url):
                invalid.append(url)
        
        return invalid
    
    def _is_valid_handle(self, value: str) -> bool:
        """
        Validate handle format (URL-friendly).
        
        Args:
            value: Handle string
            
        Returns:
            True if valid handle format
        """
        if not value:
            return False
        
        # Handle should be lowercase, alphanumeric with hyphens
        pattern = re.compile(r'^[a-z0-9-]+$')
        return bool(pattern.match(value))
    
    def check_duplicates(self, df: pd.DataFrame, field: str) -> List[int]:
        """
        Check for duplicate values in a field.
        
        Args:
            df: DataFrame to check
            field: Field name to check for duplicates
            
        Returns:
            List of row indices with duplicates
        """
        if field not in df.columns:
            return []
        
        duplicates = df[df.duplicated(subset=[field], keep=False)]
        return duplicates.index.tolist()
    
    def generate_validation_report(
        self,
        source_df: pd.DataFrame,
        shopify_df: pd.DataFrame,
        errors: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Generate a validation report.
        
        Args:
            source_df: Source DataFrame
            shopify_df: Shopify DataFrame
            errors: List of error dictionaries
            
        Returns:
            Validation report dictionary
        """
        report = {
            'source_row_count': len(source_df),
            'shopify_row_count': len(shopify_df),
            'total_errors': len(errors),
            'error_rate': len(errors) / len(source_df) * 100 if len(source_df) > 0 else 0,
            'errors_by_type': {},
            'duplicate_skus': self.check_duplicates(shopify_df, 'Variant SKU') if 'Variant SKU' in shopify_df.columns else [],
        }
        
        # Categorize errors
        for error in errors:
            error_type = error.get('type', 'unknown')
            report['errors_by_type'][error_type] = report['errors_by_type'].get(error_type, 0) + 1
        
        return report

