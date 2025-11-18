"""
Tests for Field Mapper Module
"""

import pytest
import json
import tempfile
import os
from src.mapper import FieldMapper


class TestFieldMapper:
    """Test cases for FieldMapper."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mapper = FieldMapper()
    
    def test_direct_mapping(self):
        """Test direct field mapping."""
        config = {
            "mappings": {
                "direct": {
                    "fields": {
                        "product_name": "Title",
                        "price": "Variant Price"
                    }
                },
                "default": {
                    "fields": {}
                }
            },
            "required_fields": [],
            "optional_fields": []
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config, f)
            config_path = f.name
        
        try:
            mapper = FieldMapper(config_path)
            source_row = {
                "product_name": "Test Product",
                "price": "19.99"
            }
            
            result = mapper.map_row(source_row)
            
            assert result["Title"] == "Test Product"
            assert result["Variant Price"] == "19.99"
        finally:
            if os.path.exists(config_path):
                os.unlink(config_path)
    
    def test_concatenate_mapping(self):
        """Test concatenation mapping."""
        config = {
            "mappings": {
                "concatenate": {
                    "fields": {
                        "title": {
                            "target": "Title",
                            "fields": ["brand", "model"],
                            "separator": " "
                        }
                    }
                },
                "default": {
                    "fields": {}
                }
            },
            "required_fields": [],
            "optional_fields": []
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config, f)
            config_path = f.name
        
        try:
            mapper = FieldMapper(config_path)
            source_row = {
                "brand": "Apple",
                "model": "iPhone 13"
            }
            
            result = mapper.map_row(source_row)
            
            assert result["Title"] == "Apple iPhone 13"
        finally:
            if os.path.exists(config_path):
                os.unlink(config_path)
    
    def test_default_values(self):
        """Test default value assignment."""
        config = {
            "mappings": {
                "default": {
                    "fields": {
                        "Type": "Product",
                        "Status": "active"
                    }
                }
            },
            "required_fields": [],
            "optional_fields": []
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config, f)
            config_path = f.name
        
        try:
            mapper = FieldMapper(config_path)
            source_row = {}
            
            result = mapper.map_row(source_row)
            
            assert result["Type"] == "Product"
            assert result["Status"] == "active"
        finally:
            if os.path.exists(config_path):
                os.unlink(config_path)

