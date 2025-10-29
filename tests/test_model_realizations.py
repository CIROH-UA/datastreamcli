import pytest
from ngen.config.realization import NgenRealization
from pathlib import Path

TEST_DIR   = Path(__file__).resolve().parent
NGEN_DIR     = TEST_DIR.parent / 'configs' / 'ngen'

def test_realization_parsing_sloth_nom_cfe_pet_troute():
    REALIZATION_FILE = NGEN_DIR / "realization_sloth_nom_cfe_pet_troute.json"
    try:
        serialized_realization = NgenRealization.parse_file(REALIZATION_FILE)
    except:
        raise AssertionError(f"Failed to parse NOM/CFE/PET/troute realization file: {REALIZATION_FILE}")
    
def test_realization_parsing_python_lstm_troute():
    REALIZATION_FILE = NGEN_DIR / "realization_python_lstm_troute.json"
    try:
        serialized_realization = NgenRealization.parse_file(REALIZATION_FILE)
    except:
        raise AssertionError(f"Failed to parse python lstm realization file: {REALIZATION_FILE}")    
    
def test_realization_parsing_lstm_lstm_troute():
    REALIZATION_FILE = NGEN_DIR / "realization_rust_lstm_troute.json"
    try:
        serialized_realization = NgenRealization.parse_file(REALIZATION_FILE)
    except:
        raise AssertionError(f"Failed to parse rust lstm realization file: {REALIZATION_FILE}")  