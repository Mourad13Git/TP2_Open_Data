"""Tests unitaires pour le module storage."""
import pytest
import pandas as pd
import json
from pathlib import Path
from unittest.mock import patch, mock_open

from pipeline.storage import save_raw_json, save_parquet, load_parquet
from pipeline.config import RAW_DIR, PROCESSED_DIR


class TestSaveRawJson:
    """Tests pour la fonction save_raw_json."""
    
    def test_save_raw_json_success(self, tmp_path):
        """Test de sauvegarde JSON réussie."""
        # Mock du répertoire
        with patch('pipeline.storage.RAW_DIR', tmp_path):
            data = [{"id": 1, "name": "Test"}, {"id": 2, "name": "Other"}]
            
            filepath = save_raw_json(data, "test")
            
            assert Path(filepath).exists()
            with open(filepath, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
            assert loaded_data == data
    
    def test_save_raw_json_empty(self):
        """Test avec données vides."""
        with pytest.raises(ValueError, match="Aucune donnée"):
            save_raw_json([], "test")


class TestSaveParquet:
    """Tests pour la fonction save_parquet."""
    
    def test_save_parquet_success(self, tmp_path):
        """Test de sauvegarde Parquet réussie."""
        # Mock du répertoire
        with patch('pipeline.storage.PROCESSED_DIR', tmp_path):
            df = pd.DataFrame({
                "id": [1, 2, 3],
                "name": ["A", "B", "C"],
                "value": [10.5, 20.3, 30.1]
            })
            
            filepath = save_parquet(df, "test")
            
            assert Path(filepath).exists()
            assert filepath.endswith(".parquet")
    
    def test_save_parquet_empty(self):
        """Test avec DataFrame vide."""
        df = pd.DataFrame()
        
        with pytest.raises(ValueError, match="DataFrame vide"):
            save_parquet(df, "test")


class TestLoadParquet:
    """Tests pour la fonction load_parquet."""
    
    def test_load_parquet_success(self, tmp_path):
        """Test de chargement Parquet réussi."""
        # Créer un fichier Parquet de test
        test_file = tmp_path / "test.parquet"
        df_original = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"]
        })
        df_original.to_parquet(test_file, index=False)
        
        # Charger
        df_loaded = load_parquet(str(test_file))
        
        assert isinstance(df_loaded, pd.DataFrame)
        assert len(df_loaded) == 3
        assert list(df_loaded.columns) == ["id", "name"]
        pd.testing.assert_frame_equal(df_loaded, df_original)
    
    def test_load_parquet_not_found(self):
        """Test avec fichier inexistant."""
        with pytest.raises(Exception):
            load_parquet("nonexistent.parquet")

