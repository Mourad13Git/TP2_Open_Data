"""Module de transformation et nettoyage des données."""
import logging
import pandas as pd
from typing import Optional
from litellm import completion
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def raw_to_dataframe(raw_data: list[dict]) -> pd.DataFrame:
    """
    Convertit les données brutes en DataFrame.
    
    Args:
        raw_data: Liste de dictionnaires contenant les données brutes
    
    Returns:
        DataFrame pandas
    """
    if not raw_data:
        raise ValueError("Aucune donnée à convertir")
    
    df = pd.DataFrame(raw_data)
    logger.info(f"DataFrame créé : {df.shape[0]} lignes, {df.shape[1]} colonnes")
    logger.debug(f"Colonnes : {list(df.columns)}")
    return df


def generate_cleaning_code(df: pd.DataFrame) -> str:
    """
    Utilise l'IA pour générer du code de nettoyage adapté.
    
    Args:
        df: DataFrame à nettoyer
    
    Returns:
        Code Python de nettoyage ou message d'information si IA non disponible
    """
    import os
    
    # Vérifier si une clé API est disponible
    has_api_key = bool(
        os.getenv("LITELLM_API_KEY") or 
        os.getenv("OPENAI_API_KEY") or 
        os.getenv("GEMINI_API_KEY")
    )
    
    if not has_api_key:
        logger.warning("Clé API non configurée - Impossible d'utiliser l'IA")
        return """⚠️  Clé API non configurée.

Pour utiliser l'IA pour générer du code de nettoyage, configurez une clé API dans .env :

Option 1 - Gemini (gratuit) :
  GEMINI_API_KEY=votre_cle_gemini

Option 2 - OpenAI :
  OPENAI_API_KEY=votre_cle_openai

Pour obtenir une clé Gemini gratuite :
  https://aistudio.google.com/app/apikey

Note : Le nettoyage automatique fonctionne sans l'IA !"""
    
    try:
        from litellm import completion
        
        context = f"""
        DataFrame à nettoyer :
        - Colonnes : {list(df.columns)}
        - Types : {df.dtypes.to_dict()}
        - Valeurs manquantes : {df.isnull().sum().to_dict()}
        - Échantillon : {df.head(3).to_dict()}
        """
        
        # Essayer Gemini en premier (gratuit), puis OpenAI
        model = "gemini/gemini-2.0-flash-exp"
        if os.getenv("OPENAI_API_KEY") and not os.getenv("GEMINI_API_KEY"):
            model = "gpt-3.5-turbo"
        
        response = completion(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": """Tu es un expert en data cleaning.
                    Génère du code Python/Pandas pour nettoyer ce DataFrame.
                    Le code doit être exécutable directement.
                    Inclus : gestion des valeurs manquantes, types, normalisation."""
                },
                {"role": "user", "content": f"{context}\n\nGénère le code de nettoyage complet."}
            ]
        )
        
        code = response.choices[0].message.content
        logger.info("Code de nettoyage généré par l'IA")
        return code
    except ImportError:
        logger.error("Le package litellm n'est pas installé")
        return "❌ Erreur : Le package litellm n'est pas installé."
    except Exception as e:
        error_msg = str(e)
        if "API key" in error_msg or "api key" in error_msg.lower():
            logger.error(f"Clé API invalide : {e}")
            return f"""❌ Erreur : Clé API invalide ou manquante.

Vérifiez votre fichier .env et assurez-vous que la clé API est correcte.
Détails : {error_msg}"""
        logger.warning(f"Impossible de générer le code avec l'IA: {e}")
        return f"❌ Erreur lors de l'appel à l'IA : {error_msg}"


def clean_dataframe(df: pd.DataFrame, use_ai_suggestions: bool = False) -> pd.DataFrame:
    """
    Nettoie le DataFrame.
    
    Cette fonction implémente le nettoyage standard.
    Adaptez-la selon le code généré par l'IA.
    
    Args:
        df: DataFrame à nettoyer
        use_ai_suggestions: Si True, génère et affiche des suggestions IA
    
    Returns:
        DataFrame nettoyé
    """
    if df.empty:
        logger.warning("DataFrame vide, rien à nettoyer")
        return df
    
    df_clean = df.copy()
    initial_shape = df_clean.shape
    
    logger.info("Début du nettoyage des données")
    
    # Générer des suggestions IA si demandé
    if use_ai_suggestions:
        logger.info("Génération de suggestions IA...")
        ai_code = generate_cleaning_code(df_clean)
        if ai_code:
            logger.info(f"Suggestions IA:\n{ai_code}")
    
    # --- Nettoyage standard ---
    
    # 1. Supprimer les doublons
    initial_count = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=['code'], keep='first')
    duplicates_removed = initial_count - len(df_clean)
    if duplicates_removed > 0:
        logger.info(f"Doublons supprimés : {duplicates_removed}")
    
    # 2. Gérer les valeurs manquantes
    # Colonnes texte : remplacer par "Non renseigné"
    text_cols = df_clean.select_dtypes(include=['object', 'string']).columns
    for col in text_cols:
        missing_count = df_clean[col].isnull().sum()
        if missing_count > 0:
            df_clean[col] = df_clean[col].fillna("Non renseigné")
            logger.debug(f"Colonne '{col}': {missing_count} valeurs manquantes remplacées")
    
    # Colonnes numériques : remplacer par la médiane
    num_cols = df_clean.select_dtypes(include=['number']).columns
    for col in num_cols:
        missing_count = df_clean[col].isnull().sum()
        if missing_count > 0:
            median_val = df_clean[col].median()
            if pd.notna(median_val):
                df_clean[col] = df_clean[col].fillna(median_val)
                logger.debug(f"Colonne '{col}': {missing_count} valeurs manquantes remplacées par médiane ({median_val:.2f})")
            else:
                df_clean[col] = df_clean[col].fillna(0)
                logger.debug(f"Colonne '{col}': {missing_count} valeurs manquantes remplacées par 0")
    
    # 3. Normaliser les textes (noms de produits, marques, etc.)
    text_cols_to_normalize = ['product_name', 'brands', 'categories']
    for col in text_cols_to_normalize:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip().str.lower()
            logger.debug(f"Colonne '{col}' normalisée")
    
    # 4. Convertir les types appropriés
    # Code-barres en string
    if 'code' in df_clean.columns:
        df_clean['code'] = df_clean['code'].astype(str)
    
    # Nutri-Score en catégorie ordonnée
    if 'nutriscore_grade' in df_clean.columns:
        df_clean['nutriscore_grade'] = pd.Categorical(
            df_clean['nutriscore_grade'],
            categories=['a', 'b', 'c', 'd', 'e', 'non renseigné'],
            ordered=True
        )
    
    # 5. Nettoyer les valeurs numériques (supprimer les valeurs négatives aberrantes)
    numeric_cols_to_clean = ['energy_100g', 'fat_100g', 'sugars_100g', 'salt_100g', 'proteins_100g']
    for col in numeric_cols_to_clean:
        if col in df_clean.columns:
            # Convertir en numérique, en gérant les erreurs
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            
            # Remplacer les valeurs négatives par 0
            negative_count = (df_clean[col] < 0).sum()
            if negative_count > 0:
                df_clean.loc[df_clean[col] < 0, col] = 0
                logger.debug(f"Colonne '{col}': {negative_count} valeurs négatives corrigées")
            
            # Supprimer les outliers extrêmes (au-delà de 3 écarts-types)
            if df_clean[col].notna().sum() > 0:
                mean_val = df_clean[col].mean()
                std_val = df_clean[col].std()
                if pd.notna(std_val) and std_val > 0:
                    lower_bound = mean_val - 3 * std_val
                    upper_bound = mean_val + 3 * std_val
                    outliers = ((df_clean[col] < lower_bound) | (df_clean[col] > upper_bound)).sum()
                    if outliers > 0:
                        df_clean.loc[df_clean[col] < lower_bound, col] = lower_bound
                        df_clean.loc[df_clean[col] > upper_bound, col] = upper_bound
                        logger.debug(f"Colonne '{col}': {outliers} outliers corrigés")
    
    # 6. Gérer les listes dans les colonnes (tags, etc.)
    list_cols = ['categories', 'packaging_tags', 'labels_tags', 'countries_tags']
    for col in list_cols:
        if col in df_clean.columns:
            # Convertir les listes en chaînes séparées par des virgules
            df_clean[col] = df_clean[col].apply(
                lambda x: ', '.join(x) if isinstance(x, list) else str(x)
            )
    
    final_shape = df_clean.shape
    logger.info(f"DataFrame nettoyé : {final_shape[0]} lignes, {final_shape[1]} colonnes")
    logger.info(f"Réduction : {initial_shape[0] - final_shape[0]} lignes supprimées")
    
    return df_clean


if __name__ == "__main__":
    # Test avec des données fictives
    test_data = [
        {"code": "123", "product_name": "  Test  ", "value": 10, "category": None, "energy_100g": 100},
        {"code": "123", "product_name": "Test", "value": None, "category": "A", "energy_100g": -50},
        {"code": "456", "product_name": "Other", "value": 20, "category": "B", "energy_100g": 200},
    ]
    
    df = raw_to_dataframe(test_data)
    print("\n--- DataFrame initial ---")
    print(df)
    print("\n--- Code de nettoyage suggéré par l'IA ---")
    print(generate_cleaning_code(df))
    print("\n--- Résultat du nettoyage ---")
    df_clean = clean_dataframe(df)
    print(df_clean)

