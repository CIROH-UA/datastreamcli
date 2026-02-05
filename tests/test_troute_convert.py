from datastreamcli.nc2parquet import nc2parquet
import pytest
import xarray as xr
import pandas as pd
from pathlib import Path
from tempfile import TemporaryDirectory
import boto3
from botocore import UNSIGNED
from botocore.config import Config

@pytest.fixture(scope="session")
def download_troute_files(tmp_path_factory):
    bucket_name = "ciroh-community-ngen-datastream"
    prefix = "outputs/cfe_nom/v2.2_hydrofabric/ngen.20260101/medium_range/00/1/VPU_09/ngen-run/outputs/troute/"
    
    test_data_dir = tmp_path_factory.mktemp("troute_test_data")
    nc_files = []
    
    try:
        s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                filename = key.split('/')[-1]
                if filename.endswith('.nc'):
                    nc_files.append(filename)
    
    except Exception as e:
        nc_files = ["flowveldepth.nc", "flowpathEdge.nc", "chrtout.nc"]
    
    downloaded_files = []
    s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    for nc_file in nc_files:
        output_path = test_data_dir / nc_file
        
        try:
            s3_client.download_file(bucket_name, f"{prefix}{nc_file}", str(output_path))
            downloaded_files.append(output_path)
        except Exception:
            continue
    
    if not downloaded_files:
        pytest.skip("No NetCDF files could be downloaded from S3")
    
    yield test_data_dir, downloaded_files


def test_nc2parquet_single_file(download_troute_files):
    test_data_dir, downloaded_files = download_troute_files
    
    if not downloaded_files:
        pytest.skip("No test files available")
    
    with TemporaryDirectory() as tmp_out:
        nc_file = downloaded_files[0]
        nc2parquet(str(nc_file), tmp_out)
        
        output_file = Path(tmp_out) / f"{nc_file.stem}.parquet"
        assert output_file.exists()
        
        df = pd.read_parquet(output_file)
        assert len(df) > 0
        assert len(df.columns) > 0
        
        ds = xr.open_dataset(nc_file)
        df_original = ds.to_dataframe().reset_index()
        ds.close()
        
        assert len(df) == len(df_original)
        assert list(df.columns) == list(df_original.columns)


def test_nc2parquet_directory(download_troute_files):
    test_data_dir, downloaded_files = download_troute_files
    
    if len(downloaded_files) < 2:
        pytest.skip("Need at least 2 files to test directory conversion")
    
    with TemporaryDirectory() as tmp_out:
        nc2parquet(str(test_data_dir), tmp_out)
        
        output_files = list(Path(tmp_out).glob("*.parquet"))
        assert len(output_files) == len(downloaded_files)
        
        for nc_file in downloaded_files:
            output_file = Path(tmp_out) / f"{nc_file.stem}.parquet"
            assert output_file.exists()
            
            df = pd.read_parquet(output_file)
            assert len(df) > 0


def test_nc2parquet_file_not_found():
    with TemporaryDirectory() as tmp_out:
        with pytest.raises(FileNotFoundError):
            nc2parquet("/nonexistent/file.nc", tmp_out)


def test_nc2parquet_empty_directory():
    with TemporaryDirectory() as tmp_in, TemporaryDirectory() as tmp_out:
        nc2parquet(tmp_in, tmp_out)
        
        output_files = list(Path(tmp_out).glob("*.parquet"))
        assert len(output_files) == 0


def test_nc2parquet_output_size(download_troute_files):
    test_data_dir, downloaded_files = download_troute_files
    
    if not downloaded_files:
        pytest.skip("No test files available")
    
    with TemporaryDirectory() as tmp_out:
        nc_file = downloaded_files[0]
        nc2parquet(str(nc_file), tmp_out)
        
        output_file = Path(tmp_out) / f"{nc_file.stem}.parquet"
        assert output_file.stat().st_size > 0
        assert output_file.stat().st_size < 1e9


def test_nc2parquet_data_types(download_troute_files):
    test_data_dir, downloaded_files = download_troute_files
    
    if not downloaded_files:
        pytest.skip("No test files available")
    
    with TemporaryDirectory() as tmp_out:
        nc_file = downloaded_files[0]
        
        ds = xr.open_dataset(nc_file)
        df_original = ds.to_dataframe().reset_index()
        ds.close()
        
        nc2parquet(str(nc_file), tmp_out)
        
        output_file = Path(tmp_out) / f"{nc_file.stem}.parquet"
        df_converted = pd.read_parquet(output_file)
        
        for col in df_original.select_dtypes(include=['number']).columns:
            if col in df_converted.columns:
                assert pd.api.types.is_numeric_dtype(df_converted[col])


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])