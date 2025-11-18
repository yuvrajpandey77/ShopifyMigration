"""
Field Mapper Module
Handles mapping of source fields to Shopify fields with various transformation rules.
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable
from loguru import logger
import pandas as pd


class FieldMapper:
    """Map source fields to Shopify fields using configuration rules."""
    
    def __init__(self, mapping_config_path: Optional[str] = None):
        """
        Initialize field mapper.
        
        Args:
            mapping_config_path: Path to field mapping JSON configuration
        """
        self.mapping_config = {}
        if mapping_config_path:
            self.load_mapping_config(mapping_config_path)
    
    def load_mapping_config(self, config_path: str) -> None:
        """
        Load field mapping configuration from JSON file.
        
        Args:
            config_path: Path to mapping configuration JSON file
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.mapping_config = json.load(f)
            logger.info(f"Loaded mapping configuration from {config_path}")
        except FileNotFoundError:
            logger.warning(f"Mapping config not found: {config_path}, using empty config")
            self.mapping_config = {}
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in mapping config: {e}")
            raise
    
    def map_row(self, source_row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map a single source row to Shopify format.
        
        Args:
            source_row: Dictionary representing a source row
            
        Returns:
            Dictionary representing a Shopify-formatted row
        """
        shopify_row = {}
        
        # Apply direct mappings
        direct_mappings = self.mapping_config.get('mappings', {}).get('direct', {}).get('fields', {})
        for source_field, shopify_field in direct_mappings.items():
            if source_field in source_row:
                value = source_row[source_field]
                if pd.notna(value):  # Check for NaN/None
                    shopify_row[shopify_field] = str(value).strip()
        
        # Apply concatenation mappings
        concat_mappings = self.mapping_config.get('mappings', {}).get('concatenate', {}).get('fields', {})
        for mapping in concat_mappings.values():
            target = mapping.get('target')
            fields = mapping.get('fields', [])
            separator = mapping.get('separator', ' ')
            
            values = []
            for field in fields:
                if field in source_row:
                    value = source_row[field]
                    if pd.notna(value):
                        values.append(str(value).strip())
            
            if values:
                shopify_row[target] = separator.join(values)
        
        # Apply conditional mappings
        conditional_mappings = self.mapping_config.get('mappings', {}).get('conditional', {}).get('fields', {})
        for mapping in conditional_mappings.values():
            target = mapping.get('target')
            condition = mapping.get('condition', {})
            field = condition.get('field')
            operator = condition.get('operator')
            value = condition.get('value')
            then_value = condition.get('then')
            else_value = condition.get('else')
            
            if field and field in source_row:
                source_value = source_row[field]
                condition_met = self._evaluate_condition(source_value, operator, value)
                shopify_row[target] = then_value if condition_met else else_value
        
        # Apply default values
        default_values = self.mapping_config.get('mappings', {}).get('default', {}).get('fields', {})
        for field, default_value in default_values.items():
            if field not in shopify_row:
                shopify_row[field] = default_value
        
        return shopify_row
    
    def _evaluate_condition(
        self,
        source_value: Any,
        operator: str,
        compare_value: Any
    ) -> bool:
        """
        Evaluate a condition.
        
        Args:
            source_value: Value from source data
            operator: Comparison operator (>, <, ==, !=, >=, <=, contains, empty)
            compare_value: Value to compare against
            
        Returns:
            Boolean result of condition evaluation
        """
        if pd.isna(source_value):
            source_value = None
        
        try:
            if operator == '>':
                return float(source_value) > float(compare_value)
            elif operator == '<':
                return float(source_value) < float(compare_value)
            elif operator == '>=':
                return float(source_value) >= float(compare_value)
            elif operator == '<=':
                return float(source_value) <= float(compare_value)
            elif operator == '==':
                return str(source_value) == str(compare_value)
            elif operator == '!=':
                return str(source_value) != str(compare_value)
            elif operator == 'contains':
                return str(compare_value).lower() in str(source_value).lower()
            elif operator == 'empty':
                return pd.isna(source_value) or str(source_value).strip() == ''
            else:
                logger.warning(f"Unknown operator: {operator}")
                return False
        except (ValueError, TypeError) as e:
            logger.warning(f"Error evaluating condition: {e}")
            return False
    
    def get_required_fields(self) -> List[str]:
        """
        Get list of required Shopify fields from configuration.
        
        Returns:
            List of required field names
        """
        return self.mapping_config.get('required_fields', [])
    
    def get_optional_fields(self) -> List[str]:
        """
        Get list of optional Shopify fields from configuration.
        
        Returns:
            List of optional field names
        """
        return self.mapping_config.get('optional_fields', [])

