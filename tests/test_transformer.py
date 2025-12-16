"""Tests unitaires pour le module transformer."""
import pytest
import pandas as pd
import numpy as np

from pipeline.transformer import raw_to_dataframe, clean_dataframe


class TestRawToDataframe:
    """Tests pour la fonction raw_to_dataframe."""
    
    def test_raw_to_dataframe_success(self):
        """Test de conversion réussie."""
        raw_data = [
            {"id": 1, "name": "Test", "value": 10},
            {"id": 2, "name": "Other", "value": 20}
        ]
        
        df = raw_to_dataframe(raw_data)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["id", "name", "value"]
    
    def test_raw_to_dataframe_empty(self):
        """Test avec données vides."""
        with pytest.raises(ValueError, match="Aucune donnée"):
            raw_to_dataframe([])


class TestCleanDataframe:
    """Tests pour la fonction clean_dataframe."""
    
    def test_clean_dataframe_remove_duplicates(self):
        """Test de suppression des doublons."""
        df = pd.DataFrame({
            "code": ["123", "123", "456"],
            "name": ["A", "A", "B"],
            "value": [1, 1, 2]
        })
        
        df_clean = clean_dataframe(df)
        
        assert len(df_clean) == 2
        assert "123" in df_clean["code"].values
        assert "456" in df_clean["code"].values
    
    def test_clean_dataframe_fill_missing_text(self):
        """Test de remplissage des valeurs manquantes texte."""
        df = pd.DataFrame({
            "code": ["123", "456"],
            "name": ["Test", None],
            "value": [10, 20]
        })
        
        df_clean = clean_dataframe(df)
        
        assert df_clean["name"].isnull().sum() == 0
        assert "Non renseigné" in df_clean["name"].values
    
    def test_clean_dataframe_fill_missing_numeric(self):
        """Test de remplissage des valeurs manquantes numériques."""
        df = pd.DataFrame({
            "code": ["123", "456", "789"],
            "value": [10, None, 30]
        })
        
        df_clean = clean_dataframe(df)
        
        assert df_clean["value"].isnull().sum() == 0
        # La valeur manquante devrait être remplacée par la médiane (20)
        assert df_clean["value"].notna().all()
    
    def test_clean_dataframe_normalize_text(self):
        """Test de normalisation des textes."""
        df = pd.DataFrame({
            "code": ["123"],
            "product_name": ["  TEST Product  "],
            "brands": ["  BRAND NAME  "]
        })
        
        df_clean = clean_dataframe(df)
        
        # La fonction strip() enlève les espaces, donc on vérifie sans espaces
        assert df_clean["product_name"].iloc[0] == "test product"
        assert df_clean["brands"].iloc[0] == "brand name"
    
    def test_clean_dataframe_fix_negative_values(self):
        """Test de correction des valeurs négatives."""
        df = pd.DataFrame({
            "code": ["123", "456"],
            "energy_100g": [100, -50],
            "sugars_100g": [20, -10]
        })
        
        df_clean = clean_dataframe(df)
        
        assert (df_clean["energy_100g"] >= 0).all()
        assert (df_clean["sugars_100g"] >= 0).all()
        assert df_clean.loc[df_clean["code"] == "456", "energy_100g"].iloc[0] == 0
    
    def test_clean_dataframe_empty(self):
        """Test avec DataFrame vide."""
        df = pd.DataFrame()
        
        df_clean = clean_dataframe(df)
        
        assert df_clean.empty
    
    def test_clean_dataframe_convert_code_to_string(self):
        """Test de conversion du code en string."""
        df = pd.DataFrame({
            "code": [123, 456],
            "name": ["A", "B"]
        })
        
        df_clean = clean_dataframe(df)
        
        assert df_clean["code"].dtype == "object" or df_clean["code"].dtype == "string"
        assert all(isinstance(x, str) for x in df_clean["code"])

