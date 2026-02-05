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
    Convert a NetCDF file (or directory of NetCDF files) to Parquet format.
    Args:
        nc_file: Path to input NetCDF file or directory containing NetCDF files
        out_dir: Directory to write output Parquet file(s)
    """
    
    nc_path = Path(nc_file)
    
    # Check if input exists
    if not nc_path.exists():
        raise FileNotFoundError(f"Path not found: {nc_file}")
    
    # Create output directory if it doesn't exist
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    
    # Collect NetCDF files to process
    if nc_path.is_dir():
        nc_files = sorted(nc_path.glob("*.nc"))
        if not nc_files:
            print(f"No .nc files found in directory: {nc_file}")
            return
        print(f"Found {len(nc_files)} NetCDF file(s) to convert")
    else:
        nc_files = [nc_path]
    
    # Process each file
    for idx, file_path in enumerate(nc_files, 1):
        if len(nc_files) > 1:
            print(f"\n[{idx}/{len(nc_files)}] Processing: {file_path.name}")
        else:
            print(f"Reading NetCDF file: {file_path}")
        
        # Read NetCDF file
        ds = xr.open_dataset(file_path)
        
        # Convert to DataFrame
        print("Converting to DataFrame...")
        df = ds.to_dataframe().reset_index()
        
        # Close the dataset
        ds.close()
        
        # Define output file path
        output_file = out_path / f"{file_path.stem}.parquet"
        
        # Write to Parquet with brotli compression
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
    
    if len(nc_files) > 1:
        print(f"\n✓ All {len(nc_files)} files converted successfully!")

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