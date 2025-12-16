"""Exemples d'utilisation du pipeline."""
from pipeline.main import run_pipeline
from pipeline.storage import load_parquet, list_parquet_files
import pandas as pd


def example_basic_usage():
    """Exemple d'utilisation basique du pipeline."""
    print("=" * 60)
    print("Exemple : Utilisation basique")
    print("=" * 60)
    
    # Exécuter le pipeline
    output_path = run_pipeline(
        category="chocolats",
        name="chocolats_fr"
    )
    
    print(f"\n✅ Pipeline exécuté avec succès !")
    print(f"📁 Fichier créé : {output_path}")


def example_multiple_categories():
    """Exemple de récupération de plusieurs catégories."""
    print("=" * 60)
    print("Exemple : Récupération de plusieurs catégories")
    print("=" * 60)
    
    categories = [
        ("chocolats", "chocolats_fr"),
        ("biscuits", "biscuits_fr"),
        ("boissons", "boissons_fr")
    ]
    
    for category, name in categories:
        print(f"\n🔄 Traitement de la catégorie : {category}")
        try:
            run_pipeline(category=category, name=name)
        except Exception as e:
            print(f"❌ Erreur pour {category} : {e}")
            continue


def example_load_and_analyze():
    """Exemple de chargement et analyse des données."""
    print("=" * 60)
    print("Exemple : Chargement et analyse")
    print("=" * 60)
    
    # Lister les fichiers disponibles
    files = list_parquet_files()
    
    if not files:
        print("❌ Aucun fichier Parquet trouvé. Exécutez d'abord le pipeline.")
        return
    
    # Charger le dernier fichier
    latest_file = files[-1]
    print(f"📂 Chargement de : {latest_file}")
    
    df = load_parquet(str(latest_file))
    
    print(f"\n📊 Statistiques :")
    print(f"   - Nombre de produits : {len(df)}")
    print(f"   - Colonnes : {len(df.columns)}")
    
    # Afficher un échantillon
    print(f"\n📋 Échantillon (3 premières lignes) :")
    print(df.head(3))


if __name__ == "__main__":
    print("🚀 Exemples d'utilisation du pipeline\n")
    print("Décommentez l'exemple que vous voulez exécuter dans le code.")
    print("\nExemples disponibles :")
    print("  - example_basic_usage()")
    print("  - example_multiple_categories()")
    print("  - example_load_and_analyze()")
