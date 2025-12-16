"""Module de récupération des données via API."""
import time
import logging
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm import tqdm

from .config import (
    API_BASE_URL, API_TIMEOUT, API_RATE_LIMIT, 
    PAGE_SIZE, MAX_PAGES, FIELDS, logger
)

logger = logging.getLogger(__name__)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    reraise=True
)
def fetch_page(endpoint: str, params: dict) -> dict:
    """
    Récupère une page de données avec retry automatique.
    
    Args:
        endpoint: Chemin de l'API (ex: "/search")
        params: Paramètres de la requête
    
    Returns:
        Données JSON de la réponse
    
    Raises:
        httpx.HTTPError: En cas d'erreur HTTP
        httpx.TimeoutException: En cas de timeout
    """
    url = f"{API_BASE_URL}{endpoint}"
    
    logger.debug(f"Requête vers {url} avec params: {params}")
    
    try:
        with httpx.Client(timeout=API_TIMEOUT) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            logger.debug(f"Réponse reçue : {len(data.get('products', []))} produits")
            return data
    except httpx.HTTPStatusError as e:
        logger.error(f"Erreur HTTP {e.response.status_code}: {e.response.text}")
        raise
    except httpx.TimeoutException as e:
        logger.error(f"Timeout lors de la requête: {e}")
        raise
    except Exception as e:
        logger.error(f"Erreur inattendue lors de la requête: {e}")
        raise


def fetch_all_data(category: str) -> list[dict]:
    """
    Récupère toutes les données d'une catégorie avec pagination.
    
    Args:
        category: Catégorie à récupérer (ex: "chocolats")
    
    Returns:
        Liste de tous les produits
    
    Raises:
        ValueError: Si aucune donnée n'est récupérée
    """
    all_products = []
    logger.info(f"Début de la récupération pour la catégorie: {category}")
    
    for page in tqdm(range(1, MAX_PAGES + 1), desc="Récupération", unit="page"):
        params = {
            "categories_tags": category,
            "page": page,
            "page_size": PAGE_SIZE,
            "fields": FIELDS
        }
        
        try:
            data = fetch_page("/search", params)
            products = data.get("products", [])
            
            if not products:
                logger.info(f"Plus de données à la page {page}")
                break
                
            all_products.extend(products)
            logger.debug(f"Page {page}: {len(products)} produits récupérés")
            
            # Respecter le rate limit
            if page < MAX_PAGES:
                time.sleep(API_RATE_LIMIT)
                
        except httpx.HTTPStatusError as e:
            logger.warning(f"Erreur HTTP page {page}: {e.response.status_code}")
            if e.response.status_code == 404:
                logger.info("Page non trouvée, arrêt de la pagination")
                break
            continue
        except Exception as e:
            logger.error(f"Erreur page {page}: {e}")
            continue
    
    if not all_products:
        raise ValueError(f"Aucune donnée récupérée pour la catégorie '{category}'")
    
    logger.info(f"Total récupéré : {len(all_products)} produits")
    return all_products


if __name__ == "__main__":
    # Test
    try:
        products = fetch_all_data("chocolats")
        print(f"Premier produit : {products[0] if products else 'Aucun'}")
    except Exception as e:
        logger.error(f"Erreur lors du test: {e}")

