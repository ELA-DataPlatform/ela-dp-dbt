# Plan de refactorisation du naming dbt

> État : implémentation en cours sur `feature/refactor-dbt-naming`.
>
> Portée : 151 fichiers SQL, dont 147 modèles actifs et 4 modèles désactivés.

## Convention retenue

| Couche | Nom du modèle dbt | Relation BigQuery |
|---|---|---|
| Lake | `dlk_<source>_<stg|svc>__<entity>` | `<project>.lake_<source>_<stg|svc>.<entity>` |
| Hub | `hub_<domain>_<stg|svc>__<entity>` | `<project>.hub_<domain>_<stg|svc>.<entity>` |
| Product Data4Agent | `pct_data4agent__<entity>` | `<project>.product_data4agent.<entity>` |
| Product Webapp | `pct_webapp_<domain>__<entity>` | `<project>.product_webapp.<domain>__<entity>` |

Le préfixe `pct` reste dans tous les noms de modèles Product. Dans BigQuery, le
dataset Webapp est volontairement commun à tous les domaines. Le sous-domaine
reste donc dans l'alias physique pour éviter les collisions, notamment entre
`activities__activity_list` et `activity_focus__activity_list`.

Les alias explicites ont toujours priorité. La macro d'alias ne s'applique qu'aux
modèles du package `ela_dp` et ne modifie ni les snapshots ni les packages tiers.

## Datasets cibles

La cible comporte 13 datasets dbt, sans préfixe `dp_` ni suffixe
d'environnement. L'environnement est porté par le projet GCP (`ela-dp-dev` ou
`ela-dp-prd`).

| Dataset | Modèles actifs |
|---|---:|
| `lake_garmin_stg` | 29 |
| `lake_garmin_svc` | 29 |
| `lake_spotify_stg` | 8 |
| `lake_spotify_svc` | 8 |
| `lake_spotify_legacy_stg` | 9 |
| `lake_spotify_legacy_svc` | 9 |
| `hub_activities_stg` | 1 |
| `hub_activities_svc` | 1 |
| `hub_music_stg` | 10 |
| `hub_music_svc` | 6 |
| `hub_utils_stg` | 1 |
| `product_data4agent` | 8 |
| `product_webapp` | 28 |

## Sources normalized hors périmètre

Les tables normalized restent dans leurs datasets historiques :

- `dp_lake_garmin_<env>` ;
- `dp_lake_spotify_<env>`.

Les déclarations `source()`, les pipelines d'ingestion, leurs ressources
Terraform et leurs permissions ne sont pas modifiés. Leur migration n'est pas
requise par cette refonte.

## Ordre d'exécution

1. Créer les datasets cibles en dev depuis Terraform.
2. Renommer et construire Lake par source : Garmin, Spotify, Spotify Legacy.
3. Renommer et construire Hub : Utils, Activities, Music.
4. Router et construire Product : Data4Agent puis Webapp.
5. Vérifier le manifest, l'absence de collisions, le lint et les tests.
6. Migrer les consommateurs externes vers les nouvelles relations.
7. Répéter la création et la construction en production via CI après validation.

La migration est blue/green : les anciennes relations restent disponibles tant
que leurs consommateurs ne sont pas basculés. La suppression des anciens
datasets et tables est une étape distincte, après vérification des usages.

## Avancement dev

- Infrastructure : les 13 datasets cibles existent en dev. Les cinq datasets Webapp
  spécialisés créés lors du premier passage ont été vérifiés vides, puis remplacés
  par `product_webapp`. Les premiers datasets suffixés `_dev` ont été supprimés
  après reconstruction dans les cibles finales sans suffixe. Le binding IAM Music
  a été déplacé vers `hub_music_stg` et le plan Terraform dev global ne présente
  plus aucun changement.
- Garmin : renommage, build des 58 modèles et tests ciblés terminés.
- Spotify et Spotify Legacy : renommage et build des 34 modèles terminés.
- Hub : renommage et build des 19 modèles terminés. Cinq tests Music restent en
  échec sur des anomalies déjà présentes à l'identique dans les anciennes tables
  (le test de relation track/album s'améliore de 13 à 8 lignes en échec).
- Product : routage et build des 36 modèles terminés. Les 28 tables Webapp sont
  regroupées dans `product_webapp` ; les tests Product passent avec deux
  warnings de relation artiste déjà liés à la dette Music.
- Validation statique : parse, compilation, contrôle des collisions et hooks
  pre-commit terminés avec succès.
- Production : aucun changement appliqué.
