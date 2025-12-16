# Dockerfile multi-stage pour le pipeline
FROM python:3.11-slim as builder

# Installer uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers de configuration
COPY pyproject.toml ./

# Installer les dépendances
RUN uv pip install --system -e .

# Stage final
FROM python:3.11-slim

# Installer les dépendances système nécessaires
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Créer un utilisateur non-root
RUN useradd -m -u 1000 pipeline && \
    mkdir -p /app/data /app/logs && \
    chown -R pipeline:pipeline /app

# Copier les dépendances depuis le builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Définir le répertoire de travail
WORKDIR /app

# Copier le code source
COPY pipeline/ ./pipeline/
COPY exploration_api.py ./

# Créer les répertoires nécessaires
RUN mkdir -p data/raw data/processed logs notebooks

# Changer vers l'utilisateur non-root
USER pipeline

# Variables d'environnement
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Point d'entrée
ENTRYPOINT ["python", "-m", "pipeline.main"]
CMD ["--category", "chocolats", "--name", "products"]

