import pytest
import requests
import tarfile
from datastreamcli.run_validator import validate_data_dir
import shutil
from pathlib import Path

SCRIPT_DIR   = Path(__file__).resolve().parent
DATA_DIR     = SCRIPT_DIR / 'data'
DATA_PACKAGE = "https://datastream-resources.s3.us-east-1.amazonaws.com/validator.tar.gz"
ORIGINAL_TAR = "validator_test_original.tar.gz"
ORIGINAL_TAR_PATH = DATA_DIR / ORIGINAL_TAR
TEST_DIR = DATA_DIR / "test_dir"
TEST_DATA_DIR = TEST_DIR / "ngen-run"

@pytest.fixture(autouse=True)
def ready_test_folder():
    if TEST_DIR.exists():
        shutil.rmtree(TEST_DIR)
    TEST_DIR.mkdir(parents=True, exist_ok=True)

    response = requests.get(DATA_PACKAGE, stream=True, timeout=10)
    response.raise_for_status()
    with open(ORIGINAL_TAR_PATH, 'wb') as f:
        for chunk in response.iter_content():
            f.write(chunk)

    with tarfile.open(ORIGINAL_TAR_PATH, 'r:gz') as tar:
        tar.extractall(path=TEST_DIR)


def test_missing_geopackage():
    for f in Path(TEST_DATA_DIR).glob("config/*.gpkg"):
        f.unlink()
    try:
        validate_data_dir(TEST_DATA_DIR)
        assert False
    except Exception as inst:
        assert inst.__str__() == "Did not find geopackage file in ngen-run/config!!!"

def test_duplicate_geopackage():
    geo_files = list((TEST_DATA_DIR / 'config').glob('*.gpkg'))
    shutil.copy(geo_files[0], TEST_DATA_DIR / 'config' / 'extra.gpkg')
    try:
        validate_data_dir(TEST_DATA_DIR)
        assert False
    except Exception as inst:
        assert inst.__str__() == "This run directory contains more than a single geopackage file, remove all but one."

def test_missing_realization():
    for f in Path(TEST_DATA_DIR).glob("config/*realization*.json"):
        f.unlink()
    try:
        validate_data_dir(TEST_DATA_DIR)
        assert False
    except Exception as inst:
        assert inst.__str__() == "Did not find realization file in ngen-run/config!!!"

def test_duplicate_realization():
    real_files = list((TEST_DATA_DIR / 'config').glob('*realization*.json'))
    shutil.copy(real_files[0], TEST_DATA_DIR / 'config' / 'extra_realization.json')
    try:
        validate_data_dir(TEST_DATA_DIR)
        assert False
    except Exception as inst:
        assert inst.__str__() == "This run directory contains more than a single realization file, remove all but one."


def test_missing_bmi_config():
    del_file = Path(TEST_DATA_DIR, 'config/cat_config/CFE/CFE_cat-1496145.ini')
    del_file.unlink()
    try:
        validate_data_dir(TEST_DATA_DIR)
        assert False
    except Exception as inst:
        assert inst.__str__() == "cat-1496145 -> File config/cat_config/CFE/CFE_cat-1496146.ini does not match pattern specified config/cat_config/CFE/CFE_{{id}}.ini"

def test_missing_forcings():
    for f in Path(TEST_DATA_DIR).glob("forcings/*.nc"):
        f.unlink()
    try:
        validate_data_dir(TEST_DATA_DIR)
        assert False
    except Exception as inst:
        assert inst.__str__() == f"Forcings file not found!"

def test_forcings_time_axis():
    for f in Path(TEST_DATA_DIR).glob("forcings/*.nc"):
        f.unlink()

    url = "https://ciroh-community-ngen-datastream.s3.amazonaws.com/forcings/v2.2_hydrofabric/ngen.20260101/forcing_short_range/02/ngen.t02z.short_range.forcing.f001_f018.VPU_09.nc"
    new_forcings = TEST_DATA_DIR / "forcings" / "1_forcings.nc"
    response = requests.get(url, stream=True, timeout=10)
    response.raise_for_status()
    with open(new_forcings, 'wb') as f:
        for chunk in response.iter_content():
            f.write(chunk)

    try:
        validate_data_dir(TEST_DATA_DIR)
        assert False
    except Exception as inst:
        assert inst.__str__() == f"Realization start time 2025-02-28 01:00:00+00:00 does not match forcing start time 2026-01-01 03:00:00+00:00"

def test_missing_troute_config():
    del_file = Path(TEST_DATA_DIR, 'config/ngen.yaml')
    del_file.unlink()
    try:
        validate_data_dir(TEST_DATA_DIR)
        assert False
    except Exception as inst:
        assert inst.__str__() == "t-route specified in config, but not found"