# TP2 — Pipeline d'acquisition et transformation de données

Pipeline automatisé pour récupérer des données Open Data via API, les nettoyer et les stocker dans un format optimisé pour l'analyse.

## 🎯 Objectifs

- ✅ Interroger une API REST Open Data (OpenFoodFacts)
- ✅ Gérer la pagination et les erreurs
- ✅ Transformer et nettoyer des données avec l'aide de l'IA
- ✅ Stocker des données au format Parquet
- ✅ Construire un pipeline reproductible

## 📋 Prérequis

- Python 3.10+
- [uv](https://github.com/astral-sh/uv) (gestionnaire de paquets moderne)

## 🚀 Installation

```bash
# Installer les dépendances
uv sync

# OU avec pip
pip install -r requirements.txt
```

## 📁 Structure du projet

```
tp2-pipeline/
├── pipeline/              # Modules du pipeline
│   ├── __init__.py
│   ├── config.py          # Configuration
│   ├── fetcher.py         # Récupération des données
│   ├── transformer.py     # Nettoyage et transformation
│   ├── storage.py         # Stockage Parquet
│   └── main.py            # Orchestration
├── tests/                 # Tests unitaires
├── notebooks/             # Notebooks d'exploration
│   └── exploration.ipynb
├── data/                  # Données (créé automatiquement)
│   ├── raw/               # Données brutes JSON
│   └── processed/         # Données nettoyées Parquet
└── logs/                  # Logs (créé automatiquement)
```

## 🔧 Utilisation

### Exécution du pipeline

```bash
# Catégorie par défaut (chocolats)
uv run python -m pipeline.main

# Changer de catégorie
uv run python -m pipeline.main --category biscuits --name biscuits_fr
uv run python -m pipeline.main --category boissons --name boissons_fr
uv run python -m pipeline.main --category yaourts --name yaourts_fr

# Avec suggestions IA (nécessite clé API dans .env)
uv run python -m pipeline.main --category chocolats --name chocolats_fr --ai-cleaning

# Mode verbose
uv run python -m pipeline.main --category chocolats --name chocolats_fr --verbose
```

### Catégories disponibles

- `chocolats`, `biscuits`, `boissons`, `yaourts`, `pates`, `pizzas`, `fromages`, `pain`, `cereales`, `fruits`, `legumes`, `viandes`, `poissons`

### Vérification des données

```bash
uv run python verify_data.py
```

### Exploration avec Jupyter

```bash
# Lancer Jupyter
uv run jupyter notebook

# OU JupyterLab
uv run jupyter lab
```

Puis ouvrir `notebooks/exploration.ipynb` et modifier `CATEGORY_NAME` pour changer de catégorie.

### Exploration de l'API

```bash
uv run python exploration_api.py
```

## 🧪 Tests

```bash
# Tous les tests
uv run pytest tests/ -v

# Avec couverture
uv run pytest tests/ --cov=pipeline --cov-report=html
```

## 🐳 Docker

```bash
# Construction
docker build -t tp2-pipeline .

# Exécution
docker run --rm -v $(pwd)/data:/app/data tp2-pipeline --category chocolats --name chocolats_fr

# Avec Docker Compose
docker-compose run pipeline --category chocolats --name chocolats_fr
```

## 📊 Fonctionnalités

### Acquisition des données
- Récupération paginée avec retry automatique
- Gestion des erreurs et timeouts
- Respect du rate limiting
- Logging détaillé

### Transformation
- Conversion en DataFrame pandas
- Génération de code de nettoyage avec IA (optionnel)
- Nettoyage automatique :
  - Suppression des doublons
  - Gestion des valeurs manquantes
  - Normalisation des textes
  - Correction des valeurs aberrantes
  - Gestion des outliers

### Stockage
- Sauvegarde JSON pour les données brutes
- Stockage Parquet optimisé (compression snappy)
- Timestamping automatique

## 🔍 API utilisée : OpenFoodFacts

- **Base URL** : `https://world.openfoodfacts.org/api/v2`
- **Documentation** : https://openfoodfacts.github.io/openfoodfacts-server/api/
- **Pas d'authentification requise**
- **Rate limit** : 1 requête/seconde (respecté automatiquement)

## 🎁 Bonus implémentés

### ✅ Tests unitaires (+2 points)
- Tests complets pour tous les modules
- Couverture de code avec pytest-cov

### ✅ Logging (+1 point)
- Module logging configuré
- Logs structurés dans `logs/pipeline.log`

### ✅ Dockerisation (+1 point)
- Dockerfile optimisé multi-stage
- Docker Compose pour faciliter l'utilisation

## 📝 Configuration IA (optionnel)

Le pipeline fonctionne parfaitement sans IA. Pour activer les suggestions IA :

1. Créer un fichier `.env` :
```env
GEMINI_API_KEY=votre_cle_api
```

2. Obtenir une clé gratuite : https://aistudio.google.com/app/apikey

3. Utiliser le flag `--ai-cleaning` lors de l'exécution

## 🐛 Dépannage

### Erreur de connexion API
- Vérifier votre connexion internet
- Augmenter `API_TIMEOUT` dans `pipeline/config.py`

### Pas de données récupérées
- Vérifier que la catégorie existe sur OpenFoodFacts
- Consulter les logs dans `logs/pipeline.log`

### Erreur Parquet
- Vérifier que `pyarrow` est installé
- Vérifier les permissions d'écriture dans `data/processed/`

## 📄 Licence

Ce projet est réalisé dans le cadre d'un TP pédagogique.
