#!/usr/bin/env python3
"""
nc2parquet: Convert NetCDF files to Parquet format with optimal compression.
"""

import argparse
import sys
from pathlib import Path

import xarray as xr
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def nc2parquet(nc_file: str, out_dir: str) -> None:
    """
    Convert a NetCDF file to Parquet format.
    
    Args:
        nc_file: Path to input NetCDF file
        out_dir: Directory to write output Parquet file
    """
    # Validate input file
    nc_path = Path(nc_file)
    if not nc_path.exists():
        raise FileNotFoundError(f"NetCDF file not found: {nc_file}")
    
    # Create output directory if it doesn't exist
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    
    # Read NetCDF file
    print(f"Reading NetCDF file: {nc_file}")
    ds = xr.open_dataset(nc_file)
    
    # Convert to DataFrame
    print("Converting to DataFrame...")
    df = ds.to_dataframe().reset_index()
    
    # Close the dataset
    ds.close()
    
    # Define output file path
    output_file = out_path / f"{nc_path.stem}.parquet"
    
    # Write to Parquet with brotli compression (maximum compression)
    print(f"Writing Parquet file: {output_file}")
    df.to_parquet(
        output_file,
        compression='brotli',
        index=False,
        engine='pyarrow'
    )
    
    print(f"✓ Successfully converted to: {output_file}")
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Size: {output_file.stat().st_size / 1024 / 1024:.2f} MB")


def main():
    parser = argparse.ArgumentParser(
        description='Convert NetCDF files to Parquet format with optimal compression.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  nc2parquet --nc_file data.nc --out_dir ./output
  nc2parquet --nc_file /path/to/climate_data.nc --out_dir /path/to/parquet_files
        """
    )
    
    parser.add_argument(
        '--nc_file',
        required=True,
        help='Path to input NetCDF file'
    )
    
    parser.add_argument(
        '--out_dir',
        required=True,
        help='Directory to write output Parquet file'
    )
    
    args = parser.parse_args()
    
    try:
        nc2parquet(args.nc_file, args.out_dir)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()