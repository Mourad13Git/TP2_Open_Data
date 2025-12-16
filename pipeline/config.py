"""Configuration du pipeline."""
import logging
from pathlib import Path

# Configuration du logging
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Chemins
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

# Créer les dossiers
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

logger.info(f"Répertoires créés : {DATA_DIR}, {RAW_DIR}, {PROCESSED_DIR}")

# API Configuration - OpenFoodFacts
API_BASE_URL = "https://world.openfoodfacts.org/api/v2"
API_TIMEOUT = 30
API_RATE_LIMIT = 1.0  # secondes entre chaque requête

# Paramètres d'acquisition
PAGE_SIZE = 100
MAX_PAGES = 10  # Limiter pour le TP

# Champs à récupérer pour OpenFoodFacts
FIELDS = (
    "code,product_name,brands,categories,nutriscore_grade,nova_group,"
    "energy_100g,fat_100g,sugars_100g,salt_100g,proteins_100g,"
    "ingredients_text,packaging_tags,labels_tags,countries_tags"
)

# Catégories prédéfinies disponibles sur OpenFoodFacts
AVAILABLE_CATEGORIES = [
    "chocolats",
    "biscuits",
    "boissons",
    "yaourts",
    "pates",
    "pizzas",
    "fromages",
    "pain",
    "cereales",
    "fruits",
    "legumes",
    "viandes",
    "poissons",
]

