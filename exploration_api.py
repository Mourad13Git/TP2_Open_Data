"""Exploration de l'API avec l'assistant IA."""
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

# Vérifier si une clé API est disponible
HAS_IA = bool(os.getenv("LITELLM_API_KEY") or os.getenv("OPENAI_API_KEY") or os.getenv("GEMINI_API_KEY"))


def ask_api_assistant(question: str, api_doc: str = "") -> str:
    """
    Assistant spécialisé dans les APIs.
    
    Args:
        question: Question à poser à l'assistant
        api_doc: Documentation de l'API (optionnel)
    
    Returns:
        Réponse de l'assistant ou message d'information
    """
    if not HAS_IA:
        return """⚠️  Clé API non configurée.

Pour utiliser l'assistant IA, vous devez configurer une clé API dans le fichier .env :

Option 1 - Gemini (gratuit) :
  GEMINI_API_KEY=votre_cle_gemini

Option 2 - OpenAI :
  OPENAI_API_KEY=votre_cle_openai

Option 3 - LiteLLM :
  LITELLM_API_KEY=votre_cle_litellm

Pour obtenir une clé Gemini gratuite :
  1. Allez sur https://aistudio.google.com/app/apikey
  2. Créez une clé API
  3. Ajoutez-la dans .env comme : GEMINI_API_KEY=votre_cle

Note : Le pipeline fonctionne parfaitement sans l'IA !
L'IA est uniquement utilisée pour générer des suggestions de nettoyage."""

    try:
        from litellm import completion
        
        # Essayer Gemini en premier (gratuit), puis OpenAI
        model = "gemini/gemini-2.0-flash-exp"
        if os.getenv("OPENAI_API_KEY") and not os.getenv("GEMINI_API_KEY"):
            model = "gpt-3.5-turbo"
        
        response = completion(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": """Tu es un expert en APIs REST et en data engineering.
                    Tu aides à comprendre et utiliser des APIs Open Data.
                    Génère du code Python avec httpx quand on te le demande."""
                },
                {"role": "user", "content": f"{api_doc}\n\nQuestion: {question}"}
            ]
        )
        return response.choices[0].message.content
    except ImportError:
        return "❌ Erreur : Le package litellm n'est pas installé. Exécutez : uv add litellm"
    except Exception as e:
        error_msg = str(e)
        if "API key" in error_msg or "api key" in error_msg.lower():
            return f"""❌ Erreur : Clé API invalide ou manquante.

{ask_api_assistant.__doc__ if not HAS_IA else ''}

Détails de l'erreur : {error_msg}

💡 Le pipeline fonctionne parfaitement sans l'IA !
   L'IA est uniquement utilisée pour des suggestions optionnelles."""
        return f"❌ Erreur lors de l'appel à l'IA : {error_msg}"


# Exemple avec OpenFoodFacts
API_DOC = """
API OpenFoodFacts :
- Base URL: https://world.openfoodfacts.org/api/v2
- Endpoint produits: /product/{barcode}.json
- Endpoint recherche: /search.json?categories_tags={category}&page_size={n}
- Pas d'authentification requise
- Rate limit: soyez raisonnables (1 req/sec)
"""


def test_api():
    """Test un appel simple à l'API."""
    BASE_URL = "https://world.openfoodfacts.org/api/v2"
    
    try:
        response = httpx.get(
            f"{BASE_URL}/search",
            params={
                "categories_tags": "chocolats",
                "page_size": 5,
                "fields": "code,product_name,brands,nutriscore_grade"
            },
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        print(f"Nombre de produits : {data.get('count', 'N/A')}")
        print("\nPremiers produits :")
        for product in data.get("products", [])[:5]:
            print(f"- {product.get('product_name', 'N/A')} ({product.get('brands', 'N/A')})")
        
        return data
    except Exception as e:
        print(f"Erreur lors de l'appel API : {e}")
        return None


if __name__ == "__main__":
    print("=" * 70)
    print("EXPLORATION DE L'API OpenFoodFacts")
    print("=" * 70)
    
    print("\n1. Test d'appel API direct")
    print("-" * 70)
    test_api()
    
    print("\n2. Question à l'assistant IA")
    print("-" * 70)
    
    if not HAS_IA:
        print("⚠️  Clé API non configurée - Mode sans IA")
        print("   (Le test API fonctionne toujours !)")
        print()
    
    question = "Comment récupérer les 100 premiers produits de la catégorie 'chocolats' ?"
    print(f"Question : {question}\n")
    answer = ask_api_assistant(question, API_DOC)
    print(f"Réponse :\n{answer}")
    
    if not HAS_IA:
        print("\n" + "=" * 70)
        print("💡 INFORMATIONS")
        print("=" * 70)
        print("Le pipeline fonctionne parfaitement sans l'IA !")
        print("L'IA est uniquement utilisée pour des suggestions optionnelles.")
        print("\nPour activer l'IA (optionnel) :")
        print("1. Créez un fichier .env à la racine du projet")
        print("2. Ajoutez : GEMINI_API_KEY=votre_cle")
        print("3. Obtenez une clé gratuite sur : https://aistudio.google.com/app/apikey")
        print("=" * 70)

