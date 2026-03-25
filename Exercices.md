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
- Vous voyez des tables/modèles dbt comme `analytics.dim_customers`, `analytics.fct_orders`, `analytics.rpt_customer_revenue`

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

# Exercice 2 — Ajouter une colonne dans un modèle de staging

### Objectif
Faire une transformation simple dans `stg_customers` et la propager.

### Contexte
Le modèle `dbt/models/staging/stg_customers.sql` construit `full_name`.  
On va ajouter une colonne **`is_eu`** (booléen) : `true` si `country` est dans `FR, DE, ES, IT, NL, BE, CH`, sinon `false`.

### À faire
1. Modifier `dbt/models/staging/stg_customers.sql` pour ajouter la colonne `is_eu`.
2. Exécuter dbt :
   ```bash
   docker compose run --rm demo dbt run --select stg_customers+
   ```

### ✅ Validation attendue
- La table `analytics.dim_customers` (en aval) contient aussi `is_eu` (si vous l’avez sélectionnée dans `dim_customers.sql`)

### 💡 Indice
Dans DuckDB (SQL), vous pouvez faire :
```sql
country in ('FR','DE','ES','IT','NL','BE','CH') as is_eu
```

---

# Exercice 3 — Créer un nouveau mart : KPI “revenu journalier”

### Objectif
Créer un modèle “mart” et le référencer correctement.

### Spécification
Créer un modèle `rpt_daily_revenue.sql` dans `dbt/models/marts/` avec :
- `order_date`
- `nb_orders`
- `total_revenue`

Règles :
- ne compter que les commandes `paid` ou `shipped`
- agréger par `order_date`

### À faire
1. Créer le fichier `dbt/models/marts/rpt_daily_revenue.sql`
2. Utiliser `ref('fct_orders')` (pas de lecture directe sur `raw.*`)
3. Exécuter :
   ```bash
   docker compose run --rm demo dbt run --select rpt_daily_revenue
   ```

### ✅ Validation attendue
- Une table `analytics.rpt_daily_revenue` existe
- Un `SELECT * FROM analytics.rpt_daily_revenue LIMIT 10;` retourne des lignes

---

# Exercice 4 — Documenter + tester votre nouveau modèle

### Objectif
Introduire `schema.yml` : descriptions + tests.

### À faire
1. Dans `dbt/models/schema.yml`, ajouter un bloc `models:` pour `rpt_daily_revenue`.
2. Ajouter :
   - une description du modèle
   - des descriptions de colonnes
   - des tests : `not_null` sur `order_date`, `nb_orders`, `total_revenue`
3. Lancer :
   ```bash
   docker compose run --rm demo dbt test --select rpt_daily_revenue
   ```

### ✅ Validation attendue
- Les tests passent
- `dbt test` ne retourne pas d’échec

---

# Exercice 5 — Créer un test “sur mesure” (custom SQL test)

### Objectif
Écrire un test qui reflète une règle métier.

### Spécification
Créer un test qui échoue si `total_revenue < 0` dans `rpt_daily_revenue`.

### À faire
1. Créer un fichier de test dans `dbt/tests/` par exemple :
   - `dbt/tests/test_daily_revenue_non_negative.sql`
2. Écrire une requête qui **retourne des lignes uniquement si c’est invalide** (principe dbt).
3. Lancer :
   ```bash
   docker compose run --rm demo dbt test --select test_daily_revenue_non_negative
   ```

### ✅ Validation attendue
- Le test passe avec les données générées “normales”

### 💡 Indice (structure d’un test)
Un test dbt est une requête qui retourne **les enregistrements problématiques** :
```sql
select *
from {{ ref('rpt_daily_revenue') }}
where total_revenue < 0
```

---

# Exercice 6 — Macros (Jinja) : standardiser un nettoyage

### Objectif
Créer une macro réutilisable et l’employer dans plusieurs modèles.

### Spécification
Créer une macro `clean_text(expr)` qui fait : `trim(lower(expr))`.

### À faire
1. Créer `dbt/macros/clean_text.sql`
2. Implémenter la macro :
   - nom : `clean_text`
   - paramètre : `expr`
3. Utiliser cette macro dans `stg_customers.sql` sur `first_name`, `last_name`, `email`
4. Exécuter :
   ```bash
   docker compose run --rm demo dbt run --select stg_customers+
   ```

### ✅ Validation attendue
- Les champs texte sont normalisés (lowercase + trim) dans staging / marts

---

# Exercice 7 — Docs dbt : générer la documentation

### Objectif
Comprendre docs + lineage + descriptions.

### À faire
1. Générer la doc :
   ```bash
   docker compose run --rm demo dbt docs generate
   ```
2. Accéder à la doc :
```bash
   docker compose up --build dbt-docs
   ```

### ✅ Validation attendue
- Vous voyez vos descriptions de modèles/colonnes
- Vous voyez le DAG / lineage dans la doc

---

# Exercice 8 (Bonus) — Passer un modèle en incremental

> **But pédagogique** : comprendre le pattern incremental, même si l’exemple DuckDB est petit.

### Objectif
Transformer `fct_orders` en incremental sur `order_id`.

### À faire
1. Modifier `dbt/models/marts/fct_orders.sql`
2. Ajouter en haut du modèle :
   ```jinja
   {{ config(materialized='incremental', unique_key='order_id') }}
   ```
3. Ajouter une condition incremental :
   - Exemple : ne charger que les nouvelles `order_date` si incremental
4. Exécuter 2 fois de suite :
   ```bash
   docker compose run --rm demo dbt run --select fct_orders
   docker compose run --rm demo dbt run --select fct_orders
   ```
5. Vérifier que le nombre de lignes ne double pas.

### ✅ Validation attendue
- Deux exécutions successives ne dupliquent pas les lignes dans `analytics.fct_orders`

---

# Exercice 9 (Bonus) — Faire échouer volontairement un test

### Objectif
Comprendre la valeur de `relationships` et du “fail fast”.

### À faire (au choix)
- Option A (simple) : modifier `scripts/generate_fake_data.py` pour créer quelques `orders` avec un `customer_id` inexistant.
- Option B : créer un modèle “dirty” qui casse la relation.

Puis :
```bash
docker compose run --rm demo
docker compose run --rm demo dbt test --select stg_orders
```

### ✅ Validation attendue
- Le test `relationships` échoue en montrant les clés orphelines

---

## Annexes — Requêtes utiles dans DuckDB

Ouvrir un shell SQL DuckDB :

```bash
docker compose run --rm demo duckdb /data/warehouse.duckdb
```

Puis :

```sql
SHOW ALL TABLES;

SELECT COUNT(*) FROM raw.customers;
SELECT COUNT(*) FROM raw.orders;

SELECT * FROM analytics.rpt_customer_revenue ORDER BY total_revenue DESC LIMIT 10;
SELECT * FROM analytics.rpt_daily_revenue ORDER BY order_date DESC LIMIT 10;
```

---

## Pour l’animateur (suggestions)

- Après chaque exercice : demander “qu’est-ce qui est **source** ? qu’est-ce qui est **model** ? où est la **logique métier** ?”
- Mettre l’accent sur :
  - `source()` vs `ref()`
  - tests comme **contrats**
  - staging “1:1” vs marts “business”
  - sélection (`--select`) pour limiter les coûts en prod
