# ela-dp-dbt

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

Cette architecture évite de brasser l'intégralité de la normalized à chaque run : seules les nouvelles données de la staging sont mergées dans la table de service via un incremental merge.

Les données sont stockées dans **BigQuery**.

## Sources

Sources actuellement intégrées :
- **Spotify** — alimentée et disponible dans le lake (`dp_lake_spotify_<env>`)

## Environnements

- `prod` — production
- `dev` — développement

Les datasets BigQuery suivent la convention `dp_<couche>_<source>_<env>` pour le lake et `dp_<couche>_<env>` pour le hub et le product.

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

## BigQuery

- Le projet GCP de dev est `ela-dp-dev`, celui de prod est `ela-dp-prd`.
- **Ne jamais utiliser le projet `polar-scene-*`** — c'est un projet par défaut qui n'a rien à voir avec la plateforme ELA. Toujours cibler explicitement `ela-dp-dev` ou `ela-dp-prd`.

## Documentation

Documenter autant que possible : descriptions dans les fichiers `schema.yml` pour les modèles, colonnes et sources.
