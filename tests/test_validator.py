"""
Tests for Data Validator Module
"""

import pytest
import pandas as pd
from src.validator import DataValidator


class TestDataValidator:
    """Test cases for DataValidator."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.validator = DataValidator(required_fields=["Title", "Variant Price"])
    
    def test_validate_required_fields(self):
        """Test validation of required fields."""
        row = {
            "Title": "Test Product",
            "Variant Price": "19.99"
        }
        is_valid, errors, warnings = self.validator.validate_shopify_row(row, 1)
        assert is_valid
        assert len(errors) == 0
        
        row = {
            "Title": "",
            "Variant Price": "19.99"
        }
        is_valid, errors, warnings = self.validator.validate_shopify_row(row, 1)
        assert not is_valid
        assert len(errors) > 0
    
    def test_validate_price(self):
        """Test price validation."""
        row = {"Variant Price": "19.99"}
        is_valid, errors, warnings = self.validator.validate_shopify_row(row, 1, ["Variant Price"])
        assert is_valid
        
        row = {"Variant Price": "invalid"}
        is_valid, errors, warnings = self.validator.validate_shopify_row(row, 1, ["Variant Price"])
        assert not is_valid
    
    def test_validate_inventory(self):
        """Test inventory validation."""
        row = {"Variant Inventory Qty": "100"}
        is_valid, errors, warnings = self.validator.validate_shopify_row(row, 1)
        assert is_valid
        
        row = {"Variant Inventory Qty": "-10"}
        is_valid, errors, warnings = self.validator.validate_shopify_row(row, 1)
        assert not is_valid
    
    def test_check_duplicates(self):
        """Test duplicate detection."""
        df = pd.DataFrame({
            "Variant SKU": ["SKU001", "SKU002", "SKU001", "SKU003"]
        })
        
        duplicates = self.validator.check_duplicates(df, "Variant SKU")
        assert len(duplicates) == 2  # Two rows with SKU001

