# Auteur : Arnaud STEPHAN
# Formation : dbt + Prefect + duckDB


Ce repo contient tous les outils nécessaires à la formation dbt :
- **DuckDB** contient les tables brutes (`raw.customers`, `raw.orders`)
- **dbt** construit les modèles staging et marts dans le schéma `analytics`
- **Prefect** orchestration : génération de la donnée et exécution de dbt
- **Docker Compose** maintient le tout ensemble

## Démarrage

```bash
# 1) Lancement de Prefect
docker compose up -d prefect-server

# 2) Déclenchement de tout le pipeline
docker compose run --rm demo
```

Prefect est en principe accessible via l'URL:
- http://localhost:4200

Les données duckdb sont conservées en local:
- `./data/warehouse.duckdb`

## Commandes utiles

Lancement de dbt au sein du conteneur :

```bash
docker compose run --rm demo dbt debug
docker compose run --rm demo dbt run
docker compose run --rm demo dbt test
```

Inspection des tables présentes dans duckDB (il est également possible d'y accéder via un outil comme dBeaver)

```bash
docker compose run --rm demo python scripts/query_duckdb.py
```

Création d'un nouveau dataset:

```bash
docker compose run --rm -e CUSTOMERS=50 -e ORDERS=200 -e SEED=7 demo
```

## Contenu des différents dossiers

- `scripts/generate_fake_data.py`  
  Création des tables **raw** dans DuckDB.

- `dbt/`  
  projet dbt + sources + modèles + tests.

- `flows/dbt_flow.py`  
  Flow Prefect qui:
  1) génère la donnée
  2) lance `dbt deps`, `dbt run`, `dbt test`