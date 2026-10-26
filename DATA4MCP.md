# Data product MCP

`product_data4mcp` est le contrat de lecture dédié au serveur MCP. Les modèles
`pct_data4mcp__*` consomment exclusivement le hub et ne dépendent ni de la webapp,
ni de `data4agent`. Les anciens produits restent disponibles pendant la migration.

## Tables et grains

| Table | Grain / clé | Source hub |
|---|---|---|
| `activities` | Une activité Garmin / `activity_id` | Activities master |
| `activity_intervals` | Un tour source / `interval_id` | Activities splits + fact |
| `sleep_daily` | Une nuit par date de réveil et identité source / `record_id` | Health sleep |
| `hrv_daily` | Une date et identité source / `record_id` | Health HRV |
| `rhr_daily` | Une date et identité source / `record_id` | Health RHR |
| `stress_daily` | Une date et identité source / `record_id` | Health stress |
| `training_readiness_daily` | Une date et identité source / `record_id` | Health readiness |
| `training_status_daily` | Une date et identité source / `record_id` | Health training status |
| `listening_history` | Une écoute canonique `(played_at, track_id)` / `listening_id` | Music fact + référentiels |

Les tables sont reconstruites intégralement : corrections historiques, détails
tardifs et enrichissements du catalogue restent visibles. Aucun cutoff 2025
n'est ajouté. Les partitions mensuelles utilisent la date métier de chaque table.
Les descriptions des tables et colonnes sont publiées dans BigQuery (`persist_docs`)
depuis `models/product/data4mcp/schema.yml`.

## Conventions du contrat

- Santé : `health_date` garde la date locale Garmin. Pour le sommeil, D désigne
  la nuit D-1 → D. Les identités `account`, `profile` et `user` restent distinctes ;
  `source_user_id` peut être NULL et n'est pas une identité commune entre familles.
  Ne pas joindre aveuglément les tables sur cette colonne.
- Musique : les instants sont des TIMESTAMP UTC ; `played_date` et
  `played_at_local` utilisent `Europe/Paris`, avec les changements d'heure.
- Activités : `activity_date` et les DATETIME locaux viennent du hub Garmin.
  Un DATETIME local n'est pas converti artificiellement en TIMESTAMP UTC.
- Pour N jours calendaires, le futur MCP doit filtrer entre
  `DATE_SUB(CURRENT_DATE('Europe/Paris'), INTERVAL N-1 DAY)` et
  `CURRENT_DATE('Europe/Paris')`, bornes incluses, avec N ≥ 1.
  Les tables stockent l'historique ; elles n'imposent aucune fenêtre dynamique.
- Les unités figurent dans les noms et descriptions : secondes, millisecondes,
  mètres, kilomètres, bpm, watts et points de pourcentage. Une mesure absente ou
  invalidée reste NULL. Les libellés Garmin et leur qualification sont conservés.
- `quality_status` et `quality_issues` viennent du hub pour Garmin. Pour Spotify,
  ils décrivent la complétude des métadonnées, pas une validation de l'écoute.
  Les lignes partielles ou invalides ne sont pas supprimées silencieusement.
- Une collaboration reste une seule écoute. `artists` conserve tous les artistes
  dans leur ordre ; `primary_artist_id` permet une attribution unique. Déplier
  `artists` multiplie les lignes : expliciter l'attribution avant tout classement.
- `track_duration_ms` est la durée du titre au catalogue, pas le temps réellement
  écouté. Sa somme est une estimation ; une écoute ne prouve pas une lecture complète.
- Aucun ordre de résultat n'est garanti sans `ORDER BY`. Pour les tours, utiliser
  `source_index`, pas `lap_index` qui peut être NULL ou répété.

## Fraîcheur

`_product_built_at` indique la reconstruction du produit ; `_hub_processed_at`
la reconstruction du service Garmin ; `_ingested_at` l'ingestion de la donnée
source. Pour Spotify, `_metadata_ingested_at` suit séparément les enrichissements.
Pour les activités, `provenance` conserve les ingestions des sources enfants ;
`_ingested_at` seul décrit le résumé. La dernière mesure se déduit de la date
métier et ne doit pas être confondue avec ces timestamps techniques.

## Construction et migration

```sh
make ls SELECT=tag:data4mcp ARGS='--resource-type model'
make build SELECT=tag:data4mcp TARGET=dev
```

Ces commandes supposent un hub dev déjà construit. Pour un environnement vide,
inspecter puis construire les parents avec `SELECT=+tag:data4mcp` ; ne pas lancer
cette sélection élargie par défaut. Les tags `garmin` / `spotify` permettent aussi
aux exécutions par source d'inclure le nouveau produit.

Les tests protègent les clés, les statuts et la relation tours → activités. Un test
unitaire Spotify couvre les collaborations, les métadonnées absentes, le passage
de date Europe/Paris et l'enrichissement tardif d'un artiste.

La migration du repo MCP devra remplacer les anciennes relations et adapter les
noms de colonnes. Les classements musicaux se calculeront sur `listening_history`
avec une convention explicite de période et d'attribution des artistes. Les outils
composites et le catalogue MCP viendront ensuite. IAM et bascule production
appartiennent aux étapes suivantes, dans leurs repos respectifs.
