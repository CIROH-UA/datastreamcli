import pytest
from datastreamcli.ngen_configs_gen import gen_noah_owp_confs_from_pkl, gen_petAORcfe, generate_troute_conf, gen_lstm, get_hf
from datastreamcli.noahowp_pkl import multiprocess_gen_pkl
import datetime as dt
from pathlib import Path
import shutil
import subprocess
from ngen.config.realization import NgenRealization

TEST_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TEST_DIR.parent
CONFIG_DIR = PROJECT_ROOT / "configs"
NGEN_CONFIG_DIR = CONFIG_DIR / "ngen"
DATA_DIR = TEST_DIR / "data"

LSTM_REALIZATION =  NGEN_CONFIG_DIR / "realization_rust_lstm.json"
PKL_FILE = DATA_DIR / "noah-owp-modular-init.namelist.input.pkl"

# Ensure DATA_DIR exists and is empty
if DATA_DIR.exists():
    shutil.rmtree(DATA_DIR)
DATA_DIR.mkdir(parents=True)

CONF_DIR = DATA_DIR / "cat_config"
NOAH_DIR = CONF_DIR / "NOAH-OWP-M"
CFE_DIR  = CONF_DIR / "CFE"
PET_DIR  = CONF_DIR / "PET"
LSTM_DIR  = CONF_DIR / "LSTM"

GEOPACKAGE_NAME_v21 = "palisade.gpkg"
GEOPACKAGE_NAME_v22 = "vpu-09_subset.gpkg"
GEOPACKAGE_PATH_v21 = DATA_DIR / GEOPACKAGE_NAME_v21
GEOPACKAGE_PATH_v22 = DATA_DIR / GEOPACKAGE_NAME_v22

subprocess.run([
    "curl", "-L", "-o", str(GEOPACKAGE_PATH_v21),
    f"https://ngen-datastream.s3.us-east-2.amazonaws.com/{GEOPACKAGE_NAME_v21}"
], check=True)

subprocess.run([
    "curl", "-L", "-o", str(GEOPACKAGE_PATH_v22),
    f"https://communityhydrofabric.s3.us-east-1.amazonaws.com/hydrofabrics/community/VPU/{GEOPACKAGE_NAME_v22}"
], check=True)


START    = dt.datetime.strptime("202006200100", '%Y%m%d%H%M')
END      = dt.datetime.strptime("202006200100", '%Y%m%d%H%M')

hf_v21, layers_v21, attrs_v21 = get_hf(GEOPACKAGE_PATH_v21)
hf_v22, layers_v22, attrs_v22 = get_hf(GEOPACKAGE_PATH_v22)

@pytest.fixture(autouse=True)
def clean_dir():
    if CONF_DIR.exists():
        shutil.rmtree(CONF_DIR)
    CONF_DIR.mkdir(parents=True)


def test_pkl_v21():
    multiprocess_gen_pkl(GEOPACKAGE_PATH_v21, DATA_DIR, "v2.1")
    assert PKL_FILE.exists()


def test_noah_owp_m_v21():
    NOAH_DIR.mkdir(parents=True, exist_ok=True)
    gen_noah_owp_confs_from_pkl(PKL_FILE, NOAH_DIR, START, END)
    noah_config_example = NOAH_DIR / "noah-owp-modular-init-cat-2586011.namelist.input"
    assert noah_config_example.exists()


def test_cfe_v21():
    CFE_DIR.mkdir(parents=True, exist_ok=True)
    gen_petAORcfe(hf_v21, attrs_v21, DATA_DIR, ["CFE"])
    cfe_example = CFE_DIR / "CFE_cat-2586011.ini"
    assert cfe_example.exists()


def test_pet_v21():
    PET_DIR.mkdir(parents=True, exist_ok=True)
    gen_petAORcfe(hf_v21, attrs_v21, DATA_DIR, ["PET"])
    pet_example = PET_DIR / "PET_cat-2586011.ini"
    assert pet_example.exists()


def test_routing_v21():
    max_loop_size = (END - START + dt.timedelta(hours=1)).total_seconds() / 3600
    generate_troute_conf(DATA_DIR, START, max_loop_size, GEOPACKAGE_PATH_v21)
    yml_example = DATA_DIR / "troute.yaml"
    assert yml_example.exists()


def test_pkl_v22():
    multiprocess_gen_pkl(GEOPACKAGE_PATH_v22, DATA_DIR, "v2.2")
    assert PKL_FILE.exists()


def test_noah_owp_m_v22():
    NOAH_DIR.mkdir(parents=True, exist_ok=True)
    gen_noah_owp_confs_from_pkl(PKL_FILE, NOAH_DIR, START, END)
    noah_config_example = NOAH_DIR / "noah-owp-modular-init-cat-1496145.namelist.input"
    assert noah_config_example.exists()


def test_cfe_v22():
    CFE_DIR.mkdir(parents=True, exist_ok=True)
    gen_petAORcfe(hf_v22, attrs_v22, DATA_DIR, ["CFE"])
    cfe_example = CFE_DIR / "CFE_cat-1496145.ini"
    assert cfe_example.exists()


def test_pet_v22():
    PET_DIR.mkdir(parents=True, exist_ok=True)
    gen_petAORcfe(hf_v22, attrs_v22, DATA_DIR, ["PET"])
    pet_example = PET_DIR / "PET_cat-1496145.ini"
    assert pet_example.exists()


def test_lstm_v22():
    serialized_realization = NgenRealization.parse_file(LSTM_REALIZATION)
    LSTM_DIR.mkdir(parents=True, exist_ok=True)
    gen_lstm(hf_v22, attrs_v22, DATA_DIR,serialized_realization,[0,1,2])
    lstm_example = LSTM_DIR / "cat-1496145.yml"
    assert lstm_example.exists()


def test_routing_v22():
    max_loop_size = (END - START + dt.timedelta(hours=1)).total_seconds() / 3600
    generate_troute_conf(DATA_DIR, START, max_loop_size, GEOPACKAGE_PATH_v22)
    yml_example = DATA_DIR / "troute.yaml"
    assert yml_example.exists()
