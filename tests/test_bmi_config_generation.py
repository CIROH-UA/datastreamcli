import pytest
from datastreamcli.ngen_configs_gen import gen_noah_owp_confs_from_pkl, gen_petAORcfe, generate_troute_conf
from datastreamcli.noahowp_pkl import multiprocess_pkl
import datetime as dt
from pathlib import Path
import shutil
import subprocess

TEST_DIR = Path(__file__).resolve().parent
DATA_DIR = TEST_DIR / "data"

# Ensure DATA_DIR exists and is empty
if DATA_DIR.exists():
    shutil.rmtree(DATA_DIR)
DATA_DIR.mkdir(parents=True)

CONF_DIR = DATA_DIR / "cat_config"
NOAH_DIR = CONF_DIR / "NOAH-OWP-M"
CFE_DIR  = CONF_DIR / "CFE"
PET_DIR  = CONF_DIR / "PET"

GEOPACKAGE_NAME_v21 = "palisade.gpkg"
GEOPACKAGE_NAME_v22 = "vpu-09_subset.gpkg"
GEOPACKAGE_PATH_v21 = DATA_DIR / GEOPACKAGE_NAME_v21
GEOPACKAGE_PATH_v22 = DATA_DIR / GEOPACKAGE_NAME_v22

# Download geopackages using subprocess (more portable than os.system)
subprocess.run([
    "curl", "-L", "-o", str(GEOPACKAGE_PATH_v21),
    f"https://ngen-datastream.s3.us-east-2.amazonaws.com/{GEOPACKAGE_NAME_v21}"
], check=True)

subprocess.run([
    "curl", "-L", "-o", str(GEOPACKAGE_PATH_v22),
    f"https://communityhydrofabric.s3.us-east-1.amazonaws.com/hydrofabrics/community/VPU/{GEOPACKAGE_NAME_v22}"
], check=True)

PKL_FILE = DATA_DIR / "noah-owp-modular-init.namelist.input.pkl"
START    = dt.datetime.strptime("202006200100", '%Y%m%d%H%M')
END      = dt.datetime.strptime("202006200100", '%Y%m%d%H%M')


@pytest.fixture(autouse=True)
def clean_dir():
    if CONF_DIR.exists():
        shutil.rmtree(CONF_DIR)
    CONF_DIR.mkdir(parents=True)


def test_pkl_v21():
    multiprocess_pkl(GEOPACKAGE_PATH_v21, DATA_DIR)
    assert PKL_FILE.exists()


def test_noah_owp_m_v21():
    NOAH_DIR.mkdir(parents=True, exist_ok=True)
    gen_noah_owp_confs_from_pkl(PKL_FILE, NOAH_DIR, START, END)
    noah_config_example = NOAH_DIR / "noah-owp-modular-init-cat-2586011.namelist.input"
    assert noah_config_example.exists()


def test_cfe_v21():
    CFE_DIR.mkdir(parents=True, exist_ok=True)
    gen_petAORcfe(GEOPACKAGE_PATH_v21, DATA_DIR, ["CFE"])
    cfe_example = CFE_DIR / "CFE_cat-2586011.ini"
    assert cfe_example.exists()


def test_pet_v21():
    PET_DIR.mkdir(parents=True, exist_ok=True)
    gen_petAORcfe(GEOPACKAGE_PATH_v21, DATA_DIR, ["PET"])
    pet_example = PET_DIR / "PET_cat-2586011.ini"
    assert pet_example.exists()


def test_routing_v21():
    max_loop_size = (END - START + dt.timedelta(hours=1)).total_seconds() / 3600
    generate_troute_conf(DATA_DIR, START, max_loop_size, GEOPACKAGE_PATH_v21)
    yml_example = DATA_DIR / "troute.yaml"
    assert yml_example.exists()


def test_pkl_v22():
    multiprocess_pkl(GEOPACKAGE_PATH_v22, DATA_DIR)
    assert PKL_FILE.exists()


def test_noah_owp_m_v22():
    NOAH_DIR.mkdir(parents=True, exist_ok=True)
    gen_noah_owp_confs_from_pkl(PKL_FILE, NOAH_DIR, START, END)
    noah_config_example = NOAH_DIR / "noah-owp-modular-init-cat-1496145.namelist.input"
    assert noah_config_example.exists()


def test_cfe_v22():
    CFE_DIR.mkdir(parents=True, exist_ok=True)
    gen_petAORcfe(GEOPACKAGE_PATH_v22, DATA_DIR, ["CFE"])
    cfe_example = CFE_DIR / "CFE_cat-1496145.ini"
    assert cfe_example.exists()


def test_pet_v22():
    PET_DIR.mkdir(parents=True, exist_ok=True)
    gen_petAORcfe(GEOPACKAGE_PATH_v22, DATA_DIR, ["PET"])
    pet_example = PET_DIR / "PET_cat-1496145.ini"
    assert pet_example.exists()


def test_routing_v22():
    max_loop_size = (END - START + dt.timedelta(hours=1)).total_seconds() / 3600
    generate_troute_conf(DATA_DIR, START, max_loop_size, GEOPACKAGE_PATH_v22)
    yml_example = DATA_DIR / "troute.yaml"
    assert yml_example.exists()
