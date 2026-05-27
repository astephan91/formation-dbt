# Exercices dbt — Bases (atelier sur le projet DuckDB + Prefect)

Ces exercices sont conçus pour être faits **sur le repo du workshop** (DuckDB + dbt + Prefect + Docker).  
Ils couvrent les bases dbt : **sources, models, ref, tests, docs, macros, sélection de nœuds, materializations** (et un bonus incremental).

---

## Rappels (comment exécuter dbt dans ce workshop)

Dans ce projet, dbt tourne dans Docker. Exemples :

```bash
docker compose run --rm demo dbt debug
docker compose run --rm demo dbt ls
docker compose run --rm demo dbt run
docker compose run --rm demo dbt test
```

Inspecter les données DuckDB (après un `dbt run`) :

```bash
docker compose run --rm demo python scripts/query_duckdb.py
# ou en SQL interactif :
docker compose run --rm demo duckdb /data/warehouse.duckdb
```

---

# Exercice 0 — Vérifier que tout tourne

### Objectif
Valider l’installation et comprendre le “pipeline” minimal.

### À faire
1. Lancer le pipeline complet :
   ```bash
   docker compose run --rm demo
   ```
2. Ouvrir l’UI Prefect : http://localhost:4200  
3. Vérifier que la base DuckDB existe sur la machine : `./data/warehouse.duckdb`
4. Exécuter le script de vérification :
   ```bash
   docker compose run --rm demo python scripts/query_duckdb.py
   ```

### ✅ Validation attendue
- Vous voyez `raw.customers`, `raw.orders`
- Vous voyez des tables dans duckDB

---

# Exercice 1 — Explorer le projet dbt (DAG, modèles, sélection)

### Objectif
Comprendre les commandes dbt de base et la notion de DAG.

### À faire
1. Lister les nœuds dbt :
   ```bash
   docker compose run --rm demo dbt ls
   ```
2. Lister uniquement les modèles de staging :
   ```bash
   docker compose run --rm demo dbt ls --select staging
   ```
3. Compiler un modèle sans l’exécuter :
   ```bash
   docker compose run --rm demo dbt compile --select stg_orders
   ```
4. Exécuter uniquement un modèle + ses dépendances en amont :
   ```bash
   docker compose run --rm demo dbt run --select +rpt_customer_revenue
   ```

### ✅ Validation attendue
- Vous comprenez la différence entre `compile` et `run`
- Vous voyez comment `--select` navigue dans le DAG (`+` amont/aval)

---

# Exercice 2 - Création du premier modèle

### À faire
Dans le dossier `models`, créez un fichier .sql contenant une requête quelconque, puis lancez la commande `docker compose run --rm demo dbt run`.

### ✅ Validation attendue
- Votre modèle correspond à une table/vue qui est visible dans la base duckDB.

### Questions
- Dans quel schéma est apparue la table/vue ? A quoi cela correspond-il ?
- Dans le dossier `target/`, quelle est la différence entre le code présent dans les dossiers `compiled/` et `run/`?

# Exercice 3 - Apparition des sources

### À faire
- Créez vos différents dossiers au sein du dossier `models`, et ajoutez les sources qui correspondent aux 3 tables présentes dans duckDB au sein du schéma `raw`.
- Mettez en place des tests sur ces sources, en fonction de ce qui vous semble logique (notamment les clefs primaires).
- Créez vos premiers modèles bronze basés sur ces sources.

### ✅ Validation attendue
- Le projet compile et tourne sans erreur.
- Vos différentes couches sont présentes dans duckDB.

### Bonus
- Si vous avez fini trop vite, faites en sorte de modifier les données dans duckDB pour que l'un des tests plante.

# Exercice 3.5 - Fraîcheur

La qualité de la donnée n'est pas le seul risque en data. On peut aussi être confronté à un pipeline d'ingestion qui plante sans que personne ne s'en soit rendu compte.
Pour pallier à ça, on peut définir un seuil de fraîcheur attendu pour chaque source.

Partons du principe que notre site e-commerce marche très bien, et définissons un test de fraîcheur sur `order_date`.

# Exercice 4 - Configuration des modèles

### À faire
- A partir du fichier `dbt_project.yml`, configurez vos différentes couches pour que les objets en BDD tombent bien au bon endroit.
- Toujours à partir du même fichier, changez la matérialisation de certaines des couches.

### ✅ Validation attendue
- Les différentes couches sont matérialisées dans duckDB.

### Questions
- Les schémas des différentes couches correspondent-ils exactement à ce à quoi vous vous attendiez ?
- Après ces premiers changements, avez-vous des vieux modèles présents dans duckDB, qui ne correspondent plus à ce qu'il y a dans dbt ?

# Exercice 5 - Utilisation de macros
On cherche à ajouter une colonne **`is_eu`** (booléen) : `true` si `country` est dans `FR, DE, ES, IT, NL, BE, CH`, sinon `false`.

### À faire
- Configurez une macro (dans le dossier `macros`) et servez-vous en dans un modèle.

### ✅ Validation attendue
- Le projet compile et la colonne est bien créée dans le modèle.

### Questions
- Quel aurait été un autre moyen de gérer la liste de pays appartenant à l'UE ?

### Bonus — Utilisation de `var()`
Plutôt que de coder en dur la liste de pays dans la macro, déclarez-la comme variable dans `dbt_project.yml` :

```yaml
vars:
  eu_countries: ['FR', 'DE', 'ES', 'IT', 'NL', 'BE', 'CH']
```

Puis utilisez `{{ var('eu_countries') }}` dans votre macro ou directement dans votre modèle SQL.

**Question** : quand est-il préférable d'utiliser `var()` plutôt que de coder la valeur directement dans la macro ?

# Exercice 5.5 - generate_schema_name

Pour déterminer dans quel schéma sera construit un modèle, dbt utilise la macro suivante :

```yaml
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
```

Modifiez cette macro pour que chaque modèle soit construit exactement dans le schéma spécifié dans la configuration.

# Exercice 6 - Utilisation de seeds

### À faire
Créez une seed `countries.csv` et servez vous-en pour créer les colonnes suivantes :

- `is_eu` (boolean)
- `region` (string)

### ✅ Validation attendue
La seed apparaît bien dans duckDB, et les colonnes sont alimentées.

### Questions
Si la macro et la seed donnent le même résultat, dans quel cas de figure utiliser l'une plutôt que l'autre ?

# Exercice 7 - Modèles incrémentaux
Vous voilà expert en dbt, et vous commencez à réaliser que recharger entièrement l'entrepôt de données à chaque run est peut être un peu idiot. Après tout, une fois qu'une commande est terminée, elle ne bouge plus.

### À faire
Mettez en place l'incrémentalité sur votre table des commandes, en utilisant 2 logiques différentes :
- un filtre
- une clef primaire

### ✅ Validation attendue
-  Les lignes ne sont pas doublées dans la table qui est devenue incrémentale.

# Exercice 8 - Snapshots

Dans un e-commerce, les données clients évoluent dans le temps : un client peut changer de pays, d'adresse e-mail, etc. Un modèle classique (`view` ou `table`) ne conserve que l'état actuel — l'historique est perdu.

Les **snapshots dbt** implémentent le **SCD Type 2** : pour chaque changement détecté, la ligne précédente est "fermée" (`dbt_valid_to` est renseigné) et une nouvelle ligne est créée, permettant de retracer l'historique complet.

### À faire

1. Créez un fichier `customers_snapshot.sql` dans le dossier `snapshots/` :

```sql
{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['email', 'country'],
    )
}}

select * from {{ source('raw', 'customers') }}

{% endsnapshot %}
```

2. Lancez le snapshot une première fois :
```bash
docker compose run --rm demo dbt snapshot
```

3. Vérifiez dans DuckDB que la table `snapshots.customers_snapshot` est apparue avec les colonnes `dbt_valid_from`, `dbt_valid_to` et `dbt_scd_id`.

4. Simulez un déménagement d'un client :
```sql
UPDATE raw.customers SET country = 'US' WHERE customer_id = 'C00001';
```

5. Relancez `dbt snapshot` et observez ce qui a changé dans la table snapshot pour ce client.

### ✅ Validation attendue
- La table `snapshots.customers_snapshot` est présente dans DuckDB avec les colonnes dbt.
- Après modification et re-snapshot, le client C00001 apparaît en **deux lignes** : l'ancienne (avec `dbt_valid_to` renseigné) et la nouvelle (avec `dbt_valid_to` NULL).

### Questions
- Quelle est la différence fondamentale entre un snapshot et un modèle incrémental ?
- La table `raw.customers` n'a pas de colonne `updated_at`. Quel impact cela a-t-il sur le choix de la stratégie ? Dans quel cas utiliseriez-vous `strategy='timestamp'` ?
- Que se passe-t-il si vous modifiez une colonne qui n'est **pas** dans `check_cols` ?

### Bonus
- Référencez le snapshot dans un modèle dbt avec `{{ ref('customers_snapshot') }}` pour construire une dimension client historisée.

# Exercice 9 - Boucles Jinja

### À faire
Pour réaliser une analyse sur le nombre de commandes journalières par statut, créez un modèle `fact_daily_order_status` contenant les colonnes suivantes :

- day_date
- nb_paid_orders
- nb_cancelled_orders
- nb_shipped_orders

etc.

#### Niveau 1
Vous créez toutes les colonnes à la main
#### Niveau 2
Vous créez toutes les colonnes via une boucle qui prend en paramètre la liste de tous les status
#### Niveau 3
Vous créez toutes les colonnes via une boucle qui prend en paramètre le résultat de `select distinct status from raw.orders` 

# Exercice 10 - Flux de A à Z
Vous voilà prêt.e.s à construire votre flux en entier.

### À faire
- Créez plusieurs tables permettant de suivre les performances du magasin : commandes par jour, profit, produits les plus vendus, etc.
- Ajoutez des tests à vos différents modèles. N'utilisez pas simplement les tests de base, mais profitez en pour créer un test personnalisé, qui pourrait correspondre à un cas particulier des systèmes internes.
- Documentez les modèles, et ajoutez une exposure comme si ces modèles étaient utilisés par un outil de BI ou de Data Science.

### ✅ Validation attendue
- L'architecture de votre projet est logique et compréhensible.
- Votre projet est réutilisable dans un autre contexte.
