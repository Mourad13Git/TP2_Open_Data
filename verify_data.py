"""Vérification des données avec DuckDB."""
import duckdb
from pathlib import Path
import sys
import io

# Configurer l'encodage UTF-8 pour Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')


def verify_parquet_files(category_name: str = None):
    """
    Vérifie les fichiers Parquet disponibles.
    
    Args:
        category_name: Nom de la catégorie à vérifier (optionnel)
    """
    processed_dir = Path("data/processed")
    
    if not processed_dir.exists():
        print("❌ Le répertoire data/processed n'existe pas.")
        print("   Exécutez d'abord le pipeline : uv run python -m pipeline.main")
        return None
    
    if category_name:
        parquet_files = list(processed_dir.glob(f"{category_name}_*.parquet"))
    else:
        parquet_files = list(processed_dir.glob("*.parquet"))
    
    if not parquet_files:
        if category_name:
            print(f"❌ Aucun fichier Parquet trouvé pour '{category_name}'")
            print(f"   Exécutez : uv run python -m pipeline.main --category {category_name.split('_')[0]} --name {category_name}")
        else:
            print("❌ Aucun fichier Parquet trouvé dans data/processed/")
            print("   Exécutez d'abord le pipeline : uv run python -m pipeline.main")
        return None
    
    print(f"✅ {len(parquet_files)} fichier(s) Parquet trouvé(s)")
    return parquet_files


def analyze_data(filepath: str):
    """Analyse les données avec DuckDB."""
    print("\n" + "=" * 60)
    print(f"Analyse de : {filepath}")
    print("=" * 60)
    
    con = duckdb.connect()
    
    try:
        # Statistiques générales
        print("\n📊 Statistiques générales :")
        stats = con.execute(f"""
            SELECT 
                COUNT(*) as total_produits,
                COUNT(DISTINCT brands) as marques_uniques,
                COUNT(DISTINCT categories) as categories_uniques,
                AVG(energy_100g) as energie_moyenne,
                AVG(sugars_100g) as sucres_moyens,
                AVG(fat_100g) as matieres_grasses_moyennes,
                AVG(proteins_100g) as proteines_moyennes
            FROM read_parquet('{filepath}')
        """).df()
        
        print(stats.to_string())
        
        # Distribution du Nutri-Score
        print("\n🏆 Distribution du Nutri-Score :")
        nutri = con.execute(f"""
            SELECT 
                nutriscore_grade,
                COUNT(*) as nombre,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pourcentage
            FROM read_parquet('{filepath}')
            WHERE nutriscore_grade IS NOT NULL
            GROUP BY nutriscore_grade
            ORDER BY nutriscore_grade
        """).df()
        
        if not nutri.empty:
            print(nutri.to_string(index=False))
        else:
            print("Aucune donnée Nutri-Score disponible")
        
        # Top 5 marques
        print("\n🏭 Top 5 des marques :")
        brands = con.execute(f"""
            SELECT 
                brands,
                COUNT(*) as nombre_produits
            FROM read_parquet('{filepath}')
            WHERE brands IS NOT NULL AND brands != 'Non renseigné'
            GROUP BY brands
            ORDER BY nombre_produits DESC
            LIMIT 5
        """).df()
        
        if not brands.empty:
            print(brands.to_string(index=False))
        else:
            print("Aucune donnée de marque disponible")
        
        # Qualité des données
        print("\n🔍 Qualité des données :")
        quality = con.execute(f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN product_name IS NULL OR product_name = 'Non renseigné' THEN 1 ELSE 0 END) as nom_manquant,
                SUM(CASE WHEN brands IS NULL OR brands = 'Non renseigné' THEN 1 ELSE 0 END) as marque_manquante,
                SUM(CASE WHEN nutriscore_grade IS NULL THEN 1 ELSE 0 END) as nutri_manquant,
                SUM(CASE WHEN energy_100g IS NULL OR energy_100g = 0 THEN 1 ELSE 0 END) as energie_manquante
            FROM read_parquet('{filepath}')
        """).df()
        
        print(quality.to_string(index=False))
        
        # Échantillon de données
        print("\n📋 Échantillon de données (5 premières lignes) :")
        sample = con.execute(f"""
            SELECT 
                code,
                product_name,
                brands,
                nutriscore_grade,
                sugars_100g,
                energy_100g
            FROM read_parquet('{filepath}')
            LIMIT 5
        """).df()
        
        print(sample.to_string(index=False))
        
    except Exception as e:
        print(f"❌ Erreur lors de l'analyse : {e}")
        return False
    
    return True


def main():
    """Point d'entrée principal."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Vérification des données avec DuckDB")
    parser.add_argument(
        "--category",
        default=None,
        help="Nom de la catégorie à vérifier (ex: chocolats_fr)"
    )
    
    args = parser.parse_args()
    
    print("🔍 Vérification des données avec DuckDB")
    print("=" * 60)
    
    parquet_files = verify_parquet_files(args.category)
    
    if not parquet_files:
        sys.exit(1)
    
    # Analyser le dernier fichier
    latest_file = sorted(parquet_files)[-1]
    success = analyze_data(str(latest_file))
    
    if success:
        print("\n✅ Vérification terminée avec succès !")
    else:
        print("\n❌ Erreurs lors de la vérification")
        sys.exit(1)


if __name__ == "__main__":
    main()

