# Propagation des corrections historiques

Branche issue de `main` après la livraison du hub Silver (`8b94453`).

## Comportement

Les 46 services lake actifs conservent leur grain, leurs colonnes et leur
matérialisation incrémentale. Chaque run réconcilie toutes les lignes canoniques
exposées par le staging. Une clé nouvelle ou corrigée n'est plus exclue parce
que son `_ingested_at` est inférieur ou égal au maximum d'une autre clé.
Le même filtre est retiré des deux modèles de playlists actuellement désactivés.

La macro `bigquery__get_merge_sql` active ce comportement uniquement avec
`meta.reconcile_canonical_rows: true`, hérité par les modèles du lake :

- les clés sont comparées comme des tuples JSON, sans valeur sentinelle ;
  les valeurs nulles correspondent aux groupes de déduplication du staging ;
- cette égalité permet une équijointure BigQuery, contrairement à la première
  formulation avec `IS NOT DISTINCT FROM`, qui dépassait la limite CPU sur steps ;
- seules les colonnes autorisées par `merge_update_columns` ou
  `merge_exclude_columns` participent à la détection des changements et à l'update ;
- les lignes identiques ne sont pas réécrites ; les colonnes JSON et imbriquées
  sont comparées via `TO_JSON_STRING` ;
- en dehors du lake, la macro délègue au générateur dbt standard.

Les neuf produits Activity Focus auparavant incrémentaux deviennent des tables
reconstruites intégralement : activity_list, activity_detail, deltas, hr_zones,
segments, similar_activities, splits, timeseries et music_timeline.
Les partitions et clusters déjà déclarés sont conservés.

Cette reconstruction propage les corrections des détails, du sommeil, du poids
ou des écoutes sans dépendre de l'ingestion de l'activité. Elle retire également
les anciennes lignes dérivées quand un point, un tour, une correspondance musicale
ou une activité similaire disparaît du résultat courant. Elle évite aussi de
remplacer une partition mensuelle par les seules activités récemment ingérées.

`_ingested_at` reste une date d'ingestion source : aucune date artificielle n'est
substituée pour forcer la propagation.

## Limites et coût

Le staging reste responsable de la sélection de la version canonique. Ses
snapshots « dernière ingestion » Spotify restent des snapshots : cette livraison
ne réintroduit pas les anciennes versions qu'ils n'exposent plus. Un service lake
ne supprime pas une clé absente du staging, afin de préserver son historique
d'observation. Les suppressions dérivées sont répercutées dans les produits
reconstruits, pas interprétées comme des suppressions physiques de la source.

La réconciliation lit l'historique canonique et compare les colonnes mutables.
L'absence d'updates n'équivaut donc pas à une absence de scan. Les volumes lus et
les durées doivent être pris en compte si les historiques grandissent ; une
fenêtre glissante seule réintroduirait une limite d'ancienneté aux corrections.

## Réparation initiale

La lecture des 46 services en dev et en prod a identifié un groupe de clés
dupliquées dans chacune de ces tables Garmin : body_composition, weight,
hill_score et endurance_score. Le nouveau merge empêche leur réinsertion mais
ne supprime pas les doublons déjà présents. Leur staging relit le normalized
complet et permet la reconstruction ciblée suivante :

```sh
dbt run --profiles-dir . --target dev --full-refresh --select \
  dlk_garmin_svc__body_composition dlk_garmin_svc__weight \
  dlk_garmin_svc__hill_score dlk_garmin_svc__endurance_score
```

Cette réparation a été appliquée en dev uniquement. Pour une promotion future,
exécuter la même commande avec `--target prd`, puis reconstruire les descendants.
Ne pas effectuer de full-refresh global des services Spotify : certains stagings
n'exposent que le dernier snapshot, alors que le service conserve les clés déjà
observées.

## Validation

Les exécutions utilisent `dbt run`, jamais `dbt build` ou `dbt test`.
Les tests dbt restent reportés conformément à la demande du propriétaire.
La production n'est interrogée qu'en lecture pour cet audit.

La validation finale couvre les services et tous leurs descendants :

```sh
dbt run --profiles-dir . --target dev --select \
  path:models/lake/garmin/service+ \
  path:models/lake/spotify/service+ \
  path:models/lake/spotify_legacy/service+
```

Résultat en dev : **137 modèles réussis, zéro erreur**, en 146,17 secondes.
Les 46 services lake ont affecté zéro ligne lors de ce second passage après
réconciliation et réparation. La lecture des clés après ce passage ne retrouve
aucun groupe dupliqué dans les 46 services.

Le run complet a traité 5 928 065 514 octets (environ 5,52 Gio) : c'est un point
de mesure pour cette sélection de 137 modèles, pas une estimation de chaque job
planifié. Le plus gros scan est le service Spotify album_detail (~3 Gio) ;
activity_details Garmin représente ~712 Mio. SQLFluff, le parsing dbt et le lint
YAML sont également contrôlés, sans ajout ni exécution de tests dbt.
