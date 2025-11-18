"""
Tests for Data Transformer Module
"""

import pytest
from src.transformer import DataTransformer


class TestDataTransformer:
    """Test cases for DataTransformer."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.transformer = DataTransformer()
    
    def test_transform_price(self):
        """Test price transformation."""
        row = {"Variant Price": "$19.99"}
        result = self.transformer.transform_row(row)
        assert result["Variant Price"] == "19.99"
        
        row = {"Variant Price": "29.5"}
        result = self.transformer.transform_row(row)
        assert result["Variant Price"] == "29.50"
    
    def test_transform_inventory(self):
        """Test inventory transformation."""
        row = {"Variant Inventory Qty": "100"}
        result = self.transformer.transform_row(row)
        assert result["Variant Inventory Qty"] == "100"
        
        row = {"Variant Inventory Qty": "50.5"}
        result = self.transformer.transform_row(row)
        assert result["Variant Inventory Qty"] == "50"
    
    def test_transform_handle(self):
        """Test handle transformation."""
        row = {"Handle": "My Product Name"}
        result = self.transformer.transform_row(row)
        assert result["Handle"] == "my-product-name"
        
        row = {"Handle": "Product #1!"}
        result = self.transformer.transform_row(row)
        assert result["Handle"] == "product-1"
    
    def test_transform_tags(self):
        """Test tags transformation."""
        row = {"Tags": "tag1; tag2; tag3"}
        result = self.transformer.transform_row(row)
        assert result["Tags"] == "tag1,tag2,tag3"
        
        row = {"Tags": "tag1, tag2"}
        result = self.transformer.transform_row(row)
        assert result["Tags"] == "tag1,tag2"
    
    def test_transform_boolean(self):
        """Test boolean transformation."""
        row = {"Published": "TRUE"}
        result = self.transformer.transform_row(row)
        assert result["Published"] == "TRUE"
        
        row = {"Published": "yes"}
        result = self.transformer.transform_row(row)
        assert result["Published"] == "TRUE"
        
        row = {"Published": "no"}
        result = self.transformer.transform_row(row)
        assert result["Published"] == "FALSE"

