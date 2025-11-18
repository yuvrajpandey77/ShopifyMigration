"""
CSV Handler Module
Handles reading and writing CSV files with proper encoding and error handling.
"""

import pandas as pd
import chardet
from pathlib import Path
from typing import Optional, Dict, List, Any
from loguru import logger


class CSVHandler:
    """Handle CSV file operations with encoding detection and error handling."""
    
    def __init__(self, encoding: Optional[str] = None):
        """
        Initialize CSV handler.
        
        Args:
            encoding: Optional encoding to use. If None, will auto-detect.
        """
        self.encoding = encoding
        self.detected_encoding = None
    
    def detect_encoding(self, file_path: str) -> str:
        """
        Detect the encoding of a CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Detected encoding string
        """
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # Read first 10KB for detection
                result = chardet.detect(raw_data)
                encoding = result['encoding']
                confidence = result['confidence']
                
                logger.info(f"Detected encoding: {encoding} (confidence: {confidence:.2%})")
                return encoding or 'utf-8'
        except Exception as e:
            logger.warning(f"Could not detect encoding, using UTF-8: {e}")
            return 'utf-8'
    
    def read_csv(
        self,
        file_path: str,
        encoding: Optional[str] = None,
        chunk_size: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Read CSV file with proper encoding handling.
        
        Args:
            file_path: Path to the CSV file
            encoding: Optional encoding (will detect if not provided)
            chunk_size: Optional chunk size for large files
            **kwargs: Additional arguments to pass to pd.read_csv
            
        Returns:
            DataFrame containing the CSV data
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        # Detect encoding if not provided
        if encoding is None:
            encoding = self.encoding or self.detect_encoding(str(file_path))
            self.detected_encoding = encoding
        
        logger.info(f"Reading CSV file: {file_path}")
        
        try:
            # Try reading with detected encoding
            df = pd.read_csv(
                file_path,
                encoding=encoding,
                chunksize=chunk_size,
                on_bad_lines='skip',  # Skip bad lines instead of failing
                **kwargs
            )
            
            # If chunk_size is provided, return the iterator
            if chunk_size:
                return df
            
            # Otherwise, return the full DataFrame
            logger.info(f"Successfully read {len(df)} rows from {file_path}")
            return df
            
        except UnicodeDecodeError:
            # Try UTF-8 with BOM
            logger.warning("Encoding error, trying UTF-8-BOM")
            try:
                df = pd.read_csv(
                    file_path,
                    encoding='utf-8-sig',
                    chunksize=chunk_size,
                    on_bad_lines='skip',
                    **kwargs
                )
                if chunk_size:
                    return df
                logger.info(f"Successfully read {len(df)} rows with UTF-8-BOM")
                return df
            except Exception as e:
                logger.error(f"Failed to read CSV file: {e}")
                raise
        
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise
    
    def write_csv(
        self,
        df: pd.DataFrame,
        file_path: str,
        encoding: str = 'utf-8',  # UTF-8 without BOM to match Shopify template
        index: bool = False,
        **kwargs
    ) -> None:
        """
        Write DataFrame to CSV file.
        
        Args:
            df: DataFrame to write
            file_path: Output file path
            encoding: Encoding to use (default: utf-8 to match Shopify template)
            index: Whether to write row indices
            **kwargs: Additional arguments to pass to df.to_csv
        """
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Writing CSV file: {file_path} ({len(df)} rows)")
        
        try:
            # Ensure all columns are strings and handle NaN values properly
            df = df.copy()
            for col in df.columns:
                df[col] = df[col].fillna('').astype(str)
                # Replace 'nan' string with empty string
                df[col] = df[col].replace('nan', '')
            
            df.to_csv(
                file_path,
                encoding=encoding,
                index=index,
                lineterminator='\n',  # Use Unix line endings
                **kwargs
            )
            logger.info(f"Successfully wrote CSV file: {file_path}")
        except Exception as e:
            logger.error(f"Error writing CSV file: {e}")
            raise
    
    def analyze_csv(self, file_path: str) -> Dict[str, Any]:
        """
        Analyze a CSV file and return metadata.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Dictionary with analysis results
        """
        df = self.read_csv(file_path)
        
        analysis = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': list(df.columns),
            'dtypes': df.dtypes.to_dict(),
            'missing_values': df.isnull().sum().to_dict(),
            'sample_rows': df.head(10).to_dict('records'),
        }
        
        logger.info(f"CSV Analysis: {analysis['row_count']} rows, {analysis['column_count']} columns")
        return analysis
    
    def extract_sample(
        self,
        file_path: str,
        output_path: str,
        sample_size: int = 10
    ) -> None:
        """
        Extract a sample of rows from a CSV file.
        
        Args:
            file_path: Path to source CSV file
            output_path: Path to output sample CSV file
            sample_size: Number of rows to extract
        """
        df = self.read_csv(file_path)
        
        if len(df) < sample_size:
            logger.warning(f"Source file has only {len(df)} rows, using all rows")
            sample_df = df
        else:
            sample_df = df.head(sample_size)
        
        self.write_csv(sample_df, output_path)
        logger.info(f"Extracted {len(sample_df)} rows to {output_path}")

