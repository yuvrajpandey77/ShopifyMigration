"""
Tests for CSV Handler Module
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import os

from src.csv_handler import CSVHandler


class TestCSVHandler:
    """Test cases for CSVHandler."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.handler = CSVHandler()
        self.test_data = pd.DataFrame({
            'Name': ['Product 1', 'Product 2', 'Product 3'],
            'Price': [19.99, 29.99, 39.99],
            'SKU': ['SKU001', 'SKU002', 'SKU003']
        })
    
    def test_write_and_read_csv(self):
        """Test writing and reading CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            # Write CSV
            self.handler.write_csv(self.test_data, temp_path)
            
            # Read CSV
            df = self.handler.read_csv(temp_path)
            
            # Verify data
            assert len(df) == 3
            assert list(df.columns) == ['Name', 'Price', 'SKU']
            assert df.iloc[0]['Name'] == 'Product 1'
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_analyze_csv(self):
        """Test CSV analysis."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            self.handler.write_csv(self.test_data, temp_path)
            analysis = self.handler.analyze_csv(temp_path)
            
            assert analysis['row_count'] == 3
            assert analysis['column_count'] == 3
            assert 'Name' in analysis['columns']
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_extract_sample(self):
        """Test sample extraction."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            source_path = f.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            output_path = f.name
        
        try:
            # Create larger dataset
            large_data = pd.concat([self.test_data] * 5, ignore_index=True)
            self.handler.write_csv(large_data, source_path)
            
            # Extract sample
            self.handler.extract_sample(source_path, output_path, sample_size=5)
            
            # Verify sample
            sample_df = self.handler.read_csv(output_path)
            assert len(sample_df) == 5
        finally:
            for path in [source_path, output_path]:
                if os.path.exists(path):
                    os.unlink(path)
    
    def test_file_not_found(self):
        """Test error handling for missing file."""
        with pytest.raises(FileNotFoundError):
            self.handler.read_csv('/nonexistent/file.csv')

