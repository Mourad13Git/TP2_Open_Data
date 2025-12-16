"""Module de stockage des données."""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
import pandas as pd

from .config import RAW_DIR, PROCESSED_DIR, logger

logger = logging.getLogger(__name__)


def save_raw_json(data: list[dict], name: str) -> str:
    """
    Sauvegarde les données brutes en JSON.
    
    Args:
        data: Liste de dictionnaires à sauvegarder
        name: Nom de base du fichier
    
    Returns:
        Chemin du fichier créé
    """
    if not data:
        raise ValueError("Aucune donnée à sauvegarder")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = RAW_DIR / f"{name}_{timestamp}.json"
    
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        size_mb = filepath.stat().st_size / (1024 * 1024)
        logger.info(f"Données brutes sauvegardées : {filepath} ({size_mb:.2f} MB, {len(data)} éléments)")
        return str(filepath)
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde JSON: {e}")
        raise


def save_parquet(df: pd.DataFrame, name: str) -> str:
    """
    Sauvegarde le DataFrame en Parquet.
    
    Pourquoi Parquet ?
    - Compression efficace (5-10x plus petit que CSV)
    - Types de données préservés
    - Lecture ultra-rapide (colonnar)
    - Compatible DuckDB, Spark, etc.
    
    Args:
        df: DataFrame à sauvegarder
        name: Nom de base du fichier
    
    Returns:
        Chemin du fichier créé
    """
    if df.empty:
        raise ValueError("DataFrame vide, rien à sauvegarder")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = PROCESSED_DIR / f"{name}_{timestamp}.parquet"
    
    try:
        df.to_parquet(
            filepath,
            index=False,
            compression="snappy",
            engine="pyarrow"
        )
        
        size_mb = filepath.stat().st_size / (1024 * 1024)
        logger.info(
            f"Données sauvegardées en Parquet : {filepath} "
            f"({size_mb:.2f} MB, {len(df)} lignes, {len(df.columns)} colonnes)"
        )
        return str(filepath)
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde Parquet: {e}")
        raise


def load_parquet(filepath: str) -> pd.DataFrame:
    """
    Charge un fichier Parquet.
    
    Args:
        filepath: Chemin vers le fichier Parquet
    
    Returns:
        DataFrame chargé
    """
    try:
        df = pd.read_parquet(filepath, engine="pyarrow")
        logger.info(f"Fichier Parquet chargé : {filepath} ({len(df)} lignes)")
        return df
    except Exception as e:
        logger.error(f"Erreur lors du chargement Parquet: {e}")
        raise


def list_parquet_files(pattern: Optional[str] = None) -> list[Path]:
    """
    Liste les fichiers Parquet dans le répertoire processed.
    
    Args:
        pattern: Pattern de recherche (ex: "chocolats_*")
    
    Returns:
        Liste des chemins des fichiers Parquet
    """
    if pattern:
        files = list(PROCESSED_DIR.glob(f"{pattern}.parquet"))
    else:
        files = list(PROCESSED_DIR.glob("*.parquet"))
    
    logger.debug(f"{len(files)} fichiers Parquet trouvés")
    return sorted(files)


if __name__ == "__main__":
    # Test
    test_df = pd.DataFrame({
        "a": [1, 2, 3],
        "b": ["x", "y", "z"],
        "c": [1.1, 2.2, 3.3]
    })
    
    path = save_parquet(test_df, "test")
    loaded = load_parquet(path)
    print("DataFrame sauvegardé et rechargé:")
    print(loaded)

