#!/usr/bin/env python3
"""
CSV Analysis Utility
Analyzes CSV files to understand structure and data quality.
"""

import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv
import os
from loguru import logger
from colorama import init, Fore
import json

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.csv_handler import CSVHandler

# Initialize colorama
init(autoreset=True)


def main():
    """Main analysis function."""
    parser = argparse.ArgumentParser(
        description='Analyze CSV file structure and data quality'
    )
    parser.add_argument(
        'csv_file',
        type=str,
        help='Path to CSV file to analyze'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Output JSON file for analysis results'
    )
    
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Setup logging
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO"
    )
    
    csv_path = Path(args.csv_file)
    
    if not csv_path.exists():
        print(Fore.RED + f"ERROR: CSV file not found: {csv_path}")
        sys.exit(1)
    
    print(Fore.CYAN + "=" * 60)
    print(Fore.CYAN + "CSV FILE ANALYSIS")
    print(Fore.CYAN + "=" * 60)
    print(f"File: {csv_path}")
    print()
    
    try:
        handler = CSVHandler()
        analysis = handler.analyze_csv(str(csv_path))
        
        # Display results
        print(Fore.GREEN + "✓ Analysis Complete")
        print()
        print(Fore.YELLOW + "Summary:")
        print(f"  Rows: {analysis['row_count']}")
        print(f"  Columns: {analysis['column_count']}")
        print()
        
        print(Fore.YELLOW + "Columns:")
        for i, col in enumerate(analysis['columns'], 1):
            missing = analysis['missing_values'][col]
            missing_pct = (missing / analysis['row_count'] * 100) if analysis['row_count'] > 0 else 0
            status = Fore.GREEN + "✓" if missing == 0 else Fore.YELLOW + f"⚠ ({missing} missing, {missing_pct:.1f}%)"
            print(f"  {i:2d}. {col:30s} {status}")
        
        print()
        print(Fore.YELLOW + "Data Types:")
        for col, dtype in analysis['dtypes'].items():
            print(f"  {col:30s} {str(dtype)}")
        
        print()
        print(Fore.YELLOW + "Sample Rows (first 3):")
        for i, row in enumerate(analysis['sample_rows'][:3], 1):
            print(f"  Row {i}:")
            for key, value in list(row.items())[:5]:  # Show first 5 fields
                value_str = str(value)[:50]  # Truncate long values
                print(f"    {key}: {value_str}")
            if len(row) > 5:
                print(f"    ... and {len(row) - 5} more fields")
            print()
        
        # Save to JSON if requested
        if args.output:
            output_path = Path(args.output)
            with open(output_path, 'w', encoding='utf-8') as f:
                # Convert numpy types to native Python types for JSON
                json_data = {
                    'row_count': int(analysis['row_count']),
                    'column_count': int(analysis['column_count']),
                    'columns': analysis['columns'],
                    'dtypes': {k: str(v) for k, v in analysis['dtypes'].items()},
                    'missing_values': {k: int(v) for k, v in analysis['missing_values'].items()},
                    'sample_rows': analysis['sample_rows']
                }
                json.dump(json_data, f, indent=2, default=str)
            print(Fore.GREEN + f"✓ Analysis saved to: {output_path}")
        
    except Exception as e:
        logger.exception("Analysis failed")
        print(Fore.RED + f"\nERROR: Analysis failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

