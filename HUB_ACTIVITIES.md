# Hub Activities

Le domaine publie une activité par ligne, tous sports et tout historique disponible.
Les mesures sont qualifiées dans des préparations dédiées avant assemblage. Les
produits sélectionnent les champs du master ou lisent directement un service
spécialisé. Ils gardent leurs filtres de période, leurs libellés et leurs calculs
propres à l'usage.

## Services publics

Toutes les relations ci-dessous appartiennent à `hub_activities_svc`, dans le
projet GCP de l'environnement. Les préparations sont des vues de
`hub_activities_stg`. Les services sont des tables reconstruites complètement,
pour reprendre les corrections anciennes et les détails arrivés tardivement.

| Relation | Grain | Contrat |
|---|---|---|
| `fact_activity` | Activité | Identité, sport, temps, performance commune, extensions running/cycling/strength, GPS et qualification |
| `activity_recording` | Réponse de détails / activité | Courbes, route, descripteurs, métadonnées et qualification |
| `activity_splits` | Réponse de découpage / activité | Tableaux indépendants de laps, splits typés et résumés |
| `activity_zones` | Activité présente dans le résumé ou les zones FC | Tableaux de zones FC et de puissance, avec provenance distincte |
| `activity_exercise_sets` | Réponse de séries / activité | Séries, répétitions, charge, repos et candidats d'exercice |
| `activity_weather` | Réponse météo / activité | Mesures de station, unités déclarées et valeurs sources |
| `master_activities` | Activité du fait | Assemblage public complet, sans perte de faits ni multiplication des lignes |
| `master_running_activities` | Activité des trois anciens types course | Interface compatible pour les produits existants |

`fact_activity` et `master_activities` sont partitionnés mensuellement sur
`activity_date`. Les nouveaux services sont clusterisés sur `activity_id`.

## Structure du master

```text
activity_id, activity_key, activity_date
identity              STRUCT : UUID, nom, description, lieu, appareil, propriétaire
sport                 STRUCT : famille, type Garmin, IDs et classification
timing                STRUCT : UTC, DATETIME locale, timezone ID source
performance           STRUCT : mesures communes et unités explicites
running               STRUCT nullable : allure, dynamique et fastest_splits
cycling               STRUCT nullable : mesures spécifiques disponibles
strength              STRUCT nullable : totaux de séries et répétitions
gps                   STRUCT : coordonnées de départ et d'arrivée
flags                 STRUCT : PR, favori, manuel, parent multisport, etc.
event_type            STRUCT : classification événement source
recordings            STRUCT
  timeseries[]        ARRAY<STRUCT> : mesures, index, qualité et slots sources
  route               STRUCT : bornes, extrémités et polyline[]
  metric_descriptors[]
  compteurs et disponibilité annoncés par la source
breakdown             STRUCT
  laps[]
  typed_splits[]
  split_summaries[]
zones                 STRUCT : heart_rate[] et power[]
exercise_sets[]       ARRAY<STRUCT>, avec exercises[] imbriqué
weather               STRUCT : mesures, unités, source et qualité
health_context        STRUCT : sleep, athletic_context, association, provenance[]
quality               STRUCT : qualification indépendante de chaque composant
availability          STRUCT : réponses présentes, tableaux renseignés et anomalies
provenance[]          ARRAY<STRUCT> : famille source et date d'ingestion
source_values         STRUCT : résumé source complet avant qualification
_ingested_at, _silver_processed_at
```

Les trois préparations `activity_laps`, `activity_typed_splits` et
`activity_split_summaries` restent au grain de l'élément source. Le service les
regroupe séparément en tableaux ordonnés. Leur jointure ne peut pas transformer
10 tours et 5 zones en 50 lignes.

Les types connus sont classifiés en `running`, `cycling`, `strength`, `walking`
ou `swimming`. Les autres restent dans `other`, avec leur type Garmin original
et `is_classified = FALSE`. Le running du master tous sports inclut notamment
`track_running`. La vue compatible garde volontairement les trois types
historiques `running`, `trail_running` et `treadmill_running`.

Les extensions n'inventent pas de données absentes. En particulier, aucune
cadence vélo n'est reconstruite à partir de la cadence running. Les champs
source non promus dans les blocs publics restent disponibles dans
`source_values`; cela ne constitue pas une promesse de normalisation universelle
pour tous les sports futurs.

## Qualification et valeurs absentes

Chaque famille possède `quality_status` (`valid`, `partial`, `invalid`) et
`quality_issues ARRAY<STRING>`. Les points, tours et séries ont également leur
qualification propre. Une synthèse valide ne garantit pas des détails valides.
Dans le master, le statut de premier niveau porte sur le résumé d'activité ;
`quality` expose les statuts de chaque composant.

Les contrôles restent structurels : nombres finis, durées/distance/charges
non négatives, coordonnées dans leur domaine, pourcentages bornés, ordre des
instants et unités des métriques connues. Une valeur invalide devient NULL
dans le champ qualifié et reste consultable dans sa représentation source.
Les absences facultatives ne deviennent pas des zéros.

Les tableaux répétés absents donnent `[]`. Les extensions sportives non
applicables donnent NULL. `availability.*_response_present` distingue une réponse
vide d'une réponse inexistante ; il ne prétend pas distinguer une fonctionnalité
non supportée d'une donnée pas encore récupérée. Une réponse météo peut exister
tout en ne contenant aucune mesure utile.

Les clés de point utilisent l'activité et l'offset source, jamais seulement le
timestamp. Deux points au même instant restent distincts. Les tableaux de
courbes sont triés par instant puis offset ; les timestamps invalides sont
conservés à la fin. Aucune interpolation ni suppression des pauses.

### Parsing des courbes et unités

Le lake ajoute `indexed_metric_samples`, contenant pour chaque sample les slots
`metric_index`, `source_text` et `metric_value`. Un slot JSON null reste un
STRUCT présent avec une valeur NULL. La représentation historique
`activity_detail_metrics.metrics` reste publiée pour compatibilité externe,
mais elle retire les nulls et ne doit plus servir à une lecture positionnelle.
Le nouveau hub utilise exclusivement la représentation indexée.

Les `metric_descriptors` déterminent l'index et l'unité. Les unités des métriques
connues sont vérifiées ; une unité différente masque la mesure publique et
produit une anomalie. Les métriques inconnues gardent leur valeur et leurs
métadonnées dans les slots sources. Le `unit_factor` Garmin est conservé, sans
être appliqué aveuglément : les valeurs inspectées sont déjà en secondes,
mètres et m/s malgré des facteurs d'affichage différents de 1.

Corrections d'unités : longueur de foulée source en centimètres → mètres ;
oscillation verticale source en centimètres → millimètres ; charge des séries
source en grammes → kilogrammes. Les colonnes existantes des produits et de
l'interface running conservent leurs noms, avec les unités désormais correctes.

Le `total_work` des laps ne fournit pas de marqueur d'unité fiable et ne doit pas
être renommé arbitrairement en kJ. `garmin_activity_total_work_unit` accepte `j`,
`kj` ou `kcal` pour une conversion explicitement déclarée. Par défaut (`unknown`),
`total_work_kj` reste NULL, le lap est `partial` avec
`unverified_total_work_unit`, et la valeur source reste disponible. La même
correction s'applique au champ existant du produit Data4Agent Intervals.

La requête d'ingestion actuelle demande `maxChartSize=2000` et
`maxPolylineSize=4000`. Le hub conserve les points retournés et ne revendique
pas une courbe exhaustive à une seconde. Un historique de résumés ne démontre
pas la disponibilité de tous les détails. Les enregistrements orphelins du lake
restent dans les services spécialisés ; seuls les IDs du fait ancrent le master.

### Météo

Les unités sont des contrats explicites, jamais déduites de la valeur :

- `garmin_activity_weather_temperature_unit` : `fahrenheit` par défaut, en accord
  avec la déclaration actuelle du lake ; accepte aussi `celsius` et `unknown`.
- `garmin_activity_weather_wind_unit` : `unknown` par défaut ; accepte `km_h`,
  `mph` et `m_s`. Sans contrat confirmé, `wind_speed_kph` reste NULL, la valeur
  source est conservée et `unverified_wind_unit` signale la limitation.

Ces variables décrivent les valeurs historiquement stockées. Un changement de
réglage Garmin ou de connecteur peut exiger des métadonnées d'unité par réponse,
plutôt qu'un changement global appliqué à tout l'historique.

### Identité et contexte santé

L'installation actuelle est mono-compte. `ownerId` est conservé dans l'identité
des activités ; il n'est pas assimilé aux IDs de profil des autres API Garmin.
Les rapprochements Health existants sont descriptifs à la date locale : sommeil,
readiness, statut et dernières observations de VO2max/mesures corporelles
valides à cette date. Une pesée du jour peut être postérieure au départ.

`health_context.association` expose les dates choisies, la méthode
`platform_account_local_date`, `known_before_start_guaranteed = FALSE` et
`identity_match_guaranteed = FALSE`. Les jours ambigus de sommeil/readiness/statut
ne sont pas associés arbitrairement. Les tests bloquent l'apparition de plusieurs
propriétaires d'activité ou de jours ambigus. Le multi-compte ou les recommandations
avant départ nécessiteront une résolution d'identité et un autre contrat temporel.

## Consommation et mise à jour

Les produits Data4Agent `activities` et `activity_intervals`, ainsi que la liste
Webapp, sont raccordés au hub. Leur filtre depuis 2025 et leurs contrats de
présentation restent dans Product. Les produits Activity Focus continuent à
utiliser l'interface running compatible.

Exemple de projection sans déplier les courbes :

```sql
SELECT
    activity_id,
    identity.activity_name,
    sport.type_key,
    performance.distance_m / 1000 AS distance_km,
    running.avg_pace_min_per_km,
    health_context.sleep.overall_score AS sleep_score
FROM `ela-dp-dev.hub_activities_svc.master_activities`
WHERE sport.family = 'running'
```

Exemple de lecture de courbe, sans éliminer les pauses :

```sql
SELECT a.activity_id, p.sample_index, p.timestamp_gmt, p.heart_rate_bpm, p.quality_status
FROM `ela-dp-dev.hub_activities_svc.master_activities` AS a,
    UNNEST(a.recordings.timeseries) AS p
WHERE a.activity_date = DATE '2026-10-14'
ORDER BY p.timestamp_gmt IS NULL, p.timestamp_gmt, p.sample_index
```

Pour une première construction, le lake et Health doivent exister. Les tags
`garmin` et `activities` sont hérités par tous les nouveaux modèles du domaine.
Le job existant sélectionne `tag:garmin`, dont dbt respecte l'ordre des parents.

```sh
# Migration additive du lake, puis domaine et consommateurs concernés, en dev.
uv run dbt run --profiles-dir . --target dev --select \
  dlk_garmin_stg__activity_details dlk_garmin_svc__activity_details \
  path:models/hub/activities hub_activities_svc__master_activities+ \
  pct_data4agent__activity_intervals

# Unicité, complétude, offsets, mono-compte et cas unitaires indépendants des données.
uv run dbt test --profiles-dir . --target dev --select tag:activities
```

L'ajout lake utilise `on_schema_change=append_new_columns` et réconcilie toutes
les lignes canoniques ; aucun watermark global ne peut écarter une ancienne
activité. Les tables du hub et les produits concernés sont reconstruites.
Leur promotion n'est pas atomique : prévoir une cible de validation séparée si
la conservation du dernier ensemble cohérent pendant un échec est nécessaire.
Les tests sont inclus dans l'image Docker pour permettre leur exécution réelle.

La production n'est pas modifiée par cette implémentation. Les datasets Activities
existent déjà dans Terraform ; la présence et les accès des datasets Health
doivent être vérifiés avant une promotion dans un environnement neuf.

## Validation de cette implémentation en dev

- Migration additive des deux modèles lake de détails réussie.
- Construction réussie de 36 modèles du domaine et de ses descendants, puis du
  produit Data4Agent Intervals : 37 modèles distincts concernés par cette étape.
- 40 tests de données et deux tests unitaires réussis. Ils couvrent notamment
  les clés, la complétude du master, chaque famille imbriquée, les valeurs/offsets
  des slots par comparaison au JSON source, le périmètre mono-compte, les valeurs
  manquantes, les timestamps égaux et les unités inattendues.
- SQLFluff sur les fichiers SQL modifiés, YAML lint et `git diff --check` réussis.
- Lecture finale : **513 activités uniques**, **16 types de sport**, **767 941
  points** dans le master ; 17 activités contiennent des séries d'exercices.
- Les **365 courses** du périmètre compatible conservent exactement leurs totaux
  précédents : 2 857 829,62025001 mètres et 1 165 604,4621315002 secondes.

Les valeurs météo de vent et le travail des laps dont l'unité n'est pas déclarée
restent visibles en source, avec conversion publique NULL et qualification
partielle. Les avertissements dbt sur d'anciens arguments de tests génériques
hors de ce chantier restent présents ; ils n'ont pas empêché les validations.
