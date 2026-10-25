# ela-dp-dbt

Le [README](README.md) décrit les domaines, interfaces publiques, commandes et
promotions actuels. `dbt_project.yml` et les macros de naming font autorité pour
les schémas physiques.

## Objectif

Ce repo dbt assure la transformation des données à travers toutes les couches du médaillon de la plateforme de données ELA :

1. **Lake** : `dp_lake_<source>_<env>` — données brutes ingérées depuis les sources
2. **Hub** : `dp_hub_<env>` — données nettoyées, normalisées et consolidées
3. **Product** : `dp_product_<env>` — données prêtes à la consommation (dashboards, API, etc.)

### Architecture du Lake

Le lake contient 3 sous-couches pour chaque source :

| Sous-couche | Type | Rôle |
|---|---|---|
| **normalized** | Table | Table existante alimentée en amont (données raw chargées par l'ingestion) |
| **staging** | Vue | Déduplique les données de la normalized |
| **service** | Table | Table finale dans laquelle on merge la staging à chaque `dbt run` pour garantir l'unicité |

Les services réconcilient les enregistrements canoniques, y compris les corrections
anciennes. Une matérialisation incrémentale ne signifie pas que seules les nouvelles
lignes sont lues.

Les données sont stockées dans **BigQuery**.

## Sources

Sources actuellement intégrées :
- **Spotify** — alimentée et disponible dans le lake (`dp_lake_spotify_<env>`)
- **Garmin** — activités et santé (`dp_lake_garmin_<env>`)

## Environnements

- `prd` — production
- `dev` — développement

Les sources normalized utilisent `dp_lake_<source>_<env>`. Les modèles utilisent
les schémas explicites `lake_<source>_stg` / `_svc`, `hub_<domaine>_stg` / `_svc`
et `product_<consommateur>`. La séparation dev/prd est portée par les projets GCP.

## Contrôles locaux

- `make check` : lint SQL/YAML et parsing dbt, sans écriture BigQuery.
- `make hooks` : hooks sur tous les fichiers suivis.
- `make fix-sql` : formatage SQL explicite.
- `make docker-build`, `make deps`, puis `make run SELECT=...` : dbt dans l'image locale.
- `make parse` : parsing dans Docker ; `make parse-local` : parsing via uv pour la CI.
- `make docs` puis `make docs-serve` : catalogue dev et documentation locale dans Docker.

SQLFluff utilise le templater Jinja et la version de `uv.lock`, via
`scripts/sqlfluff.sh` en local, dans les hooks et en CI. Ne pas réintroduire un
templater ou une version distincts dans les hooks. Utiliser `uv run --locked`
pour les commandes dbt ; `make check` ne remplace pas une exécution en dev.

## Git workflow

| Branche | Rôle | Déploiement |
|---|---|---|
| `main` | Branche protégée et sacrée. Ne jamais push directement. | Cloud Run de **production** |
| `develop` | Branche d'intégration pour le développement | Cloud Run de **dev** |
| `feature/xxx` | Branches de travail pour chaque nouvelle feature | Aucun déploiement automatique |

### Règles

- **Ne jamais committer directement sur `main`**. Toute modification passe par une PR `develop` -> `main`.
- Les features sont développées sur `feature/xxx`, puis mergées dans `develop` via PR.
- `main` alimente les données de production — toute modification doit être validée.

## Tags dbt

- `spotify` : à appliquer sur **tous** les modèles qui consomment la donnée Spotify. Permet de lancer un run ciblé : `dbt run --select tag:spotify`.
- `garmin` : modèles Garmin ; tags de domaine hérités `health` et `activities`.

## BigQuery

- Le projet GCP de dev est `ela-dp-dev`, celui de prod est `ela-dp-prd`.
- **Ne jamais utiliser le projet `polar-scene-*`** — c'est un projet par défaut qui n'a rien à voir avec la plateforme ELA. Toujours cibler explicitement `ela-dp-dev` ou `ela-dp-prd`.

## Règles d'architecture

- **Les modèles de la couche product ne peuvent pas consommer d'autres modèles de la couche product.** Ils ne peuvent référencer que des modèles du hub (ou du lake en dernier recours). Toute logique partagée entre modèles product doit remonter dans le hub.

## Documentation

Documenter autant que possible : descriptions dans les fichiers `schema.yml` pour les modèles, colonnes et sources.
