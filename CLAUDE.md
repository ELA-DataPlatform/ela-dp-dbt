# ela-dp-dbt

## Objectif

Ce repo dbt assure la transformation des données à travers toutes les couches du médaillon de la plateforme de données ELA :

1. **Lake** : `dp_lake_<source>_<env>` — données brutes ingérées depuis les sources
2. **Hub** : `dp_hub_<env>` — données nettoyées, normalisées et consolidées
3. **Product** : `dp_product_<env>` — données prêtes à la consommation (dashboards, API, etc.)

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
