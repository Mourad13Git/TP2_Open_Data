"""Script principal du pipeline."""
import argparse
import logging
import sys

from .fetcher import fetch_all_data
from .transformer import raw_to_dataframe, clean_dataframe
from .storage import save_raw_json, save_parquet
from .config import logger, AVAILABLE_CATEGORIES

logger = logging.getLogger(__name__)


def run_pipeline(category: str, name: str, use_ai_cleaning: bool = False) -> str:
    """
    Exécute le pipeline complet.
    
    Args:
        category: Catégorie à récupérer
        name: Nom pour les fichiers de sortie
        use_ai_cleaning: Si True, utilise l'IA pour générer des suggestions de nettoyage
    
    Returns:
        Chemin du fichier Parquet créé
    """
    logger.info("=" * 50)
    logger.info(f"PIPELINE : {name}")
    logger.info("=" * 50)
    
    try:
        # Étape 1 : Acquisition
        logger.info("\n📥 Étape 1 : Acquisition des données")
        raw_data = fetch_all_data(category)
        raw_file = save_raw_json(raw_data, name)
        logger.info(f"✅ Données brutes sauvegardées : {raw_file}")
        
        # Étape 2 : Transformation
        logger.info("\n🔧 Étape 2 : Transformation")
        df = raw_to_dataframe(raw_data)
        df_clean = clean_dataframe(df, use_ai_suggestions=use_ai_cleaning)
        logger.info(f"✅ Données transformées : {df_clean.shape}")
        
        # Étape 3 : Stockage
        logger.info("\n💾 Étape 3 : Stockage")
        output_path = save_parquet(df_clean, name)
        logger.info(f"✅ Données sauvegardées : {output_path}")
        
        logger.info("\n" + "=" * 50)
        logger.info("✅ Pipeline terminé avec succès !")
        logger.info(f"📁 Fichier : {output_path}")
        logger.info("=" * 50)
        
        return output_path
        
    except ValueError as e:
        logger.error(f"❌ Erreur de validation : {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'exécution du pipeline : {e}", exc_info=True)
        sys.exit(1)


def main():
    """Point d'entrée principal."""
    parser = argparse.ArgumentParser(
        description="Pipeline Open Data - Acquisition et transformation de données",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python -m pipeline.main --category chocolats --name chocolats_fr
  python -m pipeline.main --category biscuits --name biscuits_fr --ai-cleaning
        """
    )
    parser.add_argument(
        "--category",
        default="chocolats",
        help=f"Catégorie à récupérer (défaut: chocolats). Catégories disponibles: {', '.join(AVAILABLE_CATEGORIES[:5])}..."
    )
    parser.add_argument(
        "--name",
        default="products",
        help="Nom du dataset (défaut: products)"
    )
    parser.add_argument(
        "--ai-cleaning",
        action="store_true",
        help="Utiliser l'IA pour générer des suggestions de nettoyage"
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Mode verbose (niveau DEBUG)"
    )
    
    args = parser.parse_args()
    
    # Ajuster le niveau de log
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    run_pipeline(args.category, args.name, args.ai_cleaning)


if __name__ == "__main__":
    main()

