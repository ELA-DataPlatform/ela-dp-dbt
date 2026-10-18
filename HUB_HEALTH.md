# Hub santé Garmin

Implémentation du 18 octobre 2026, dans la continuité du P1 du hub.
Le domaine est déclaré dans `dbt_project.yml` avec les tags `garmin` et `health`.

## Périmètre

13 familles, 21 vues de préparation dans `hub_health_stg` et 13 tables publiques
dans `hub_health_svc`. Les vues comprennent la qualification quotidienne, les
réponses sans mesure corporelle individuelle et sept préparations de points.
L'environnement reste porté par le projet GCP.

| Service `hub_health_svc__…` | Grain | Contenu |
|---|---|---|
| `sleep_daily` | Compte / date de réveil | Sommeil et phases en secondes, score, HRV nocturne, SpO₂, respiration et bornes de nuit |
| `hrv_daily` | Profil source / date | HRV et baseline en millisecondes |
| `rhr_daily` | Profil source / date | Fréquence cardiaque au repos en bpm |
| `heart_rate_daily` | Profil source / date | Minimum, maximum, repos et nombre de points source |
| `stress_daily` | Profil source / date | Scores de stress et fenêtre de mesure |
| `body_battery_daily` | Compte / date | Recharge, décharge, extrema et première/dernière valeur horodatée disponible |
| `respiration_daily` | Profil source / date | Respiration éveillée/nocturne et extrema en respirations par minute |
| `spo2_daily` | Profil source / date | Saturation quotidienne/nocturne et dernière lecture, en pourcentage |
| `training_readiness_daily` | Profil source / date | Readiness, facteurs, récupération et timestamp du snapshot disponible |
| `training_status_daily` | Utilisateur source / date | Code et libellé Garmin, charges aiguë/chronique et ACWR |
| `vo2max_daily` | Utilisateur source / date | VO₂max générique et date de mesure ; aucune propagation de valeur dans ce service |
| `wellness_daily` | Profil source / date | Pas, distance, calories, étages, minutes d'intensité et durées quotidiennes |
| `body_measurement` | Compte / identifiant de mesure | Réconciliation poids/composition, poids et masses en kg, composition en pourcentage |

Le silver conserve tout l'historique disponible. La restriction depuis 2025 des
produits Data4Agent reste dans le gold.

## Contrat commun

- `health_date` est la date locale fournie par la source. Pour le sommeil, D
  désigne la nuit D-1 → D. Les services sont partitionnés au mois sur cette date.
- `source_user_id` conserve l'identifiant source sous forme de chaîne.
  `identity_scope` distingue `profile`, `user` et `account`. Ces identifiants ne
  constituent pas une résolution d'identité entre les différentes API Garmin.
  Les sources sommeil, Body Battery et poids ne donnent pas d'identifiant exploitable :
  elles restent au grain du compte, sans identifiant utilisateur inventé.
- `source_system`, `source_table` et `source_record_key` donnent la provenance.
- Les instants sont des `TIMESTAMP` UTC ; les heures locales sont des `DATETIME`
  conservant l'heure telle qu'elle était stockée dans la source. Aucun fuseau
  d'appareil n'est déduit de la seule date ou d'une préférence Europe/Paris.
- `source_values` conserve les valeurs normalisées avant application des règles
  de domaine, ainsi que les chaînes temporelles originales pertinentes. Il ne
  remplace pas le payload d'origine du lake.
- `quality_status` vaut `valid`, `partial` ou `invalid` ; `quality_issues`
  détaille les raisons. Ces règles de qualification ne sont pas des tests dbt
  ni un blocage de publication.
- `_ingested_at` désigne l'ingestion source ; `_silver_processed_at` désigne la
  reconstruction du service. La date de traitement n'est pas une date d'observation.

Les domaines contrôlés restent structurels : scores dans [0,100], pourcentages
dans leurs bornes, fréquences et poids positifs, durées non négatives, ordre de
certaines bornes. Une valeur hors domaine devient NULL dans la colonne qualifiée,
reste visible dans `source_values` et produit un motif d'anomalie.
Une mesure principale absente donne `partial`. Les champs facultatifs peuvent
rester absents. Aucune valeur manquante n'est remplacée par zéro.

Les tables sont reconstruites complètement : corrections anciennes et changements
dans les deux sources de composition corporelle sont repris sans watermark global.
La sélection d'un enregistrement est déterministe, à partir de la clé, de la date
d'ingestion et du contenu. Les lignes présentant une clé incomplète restent
identifiables comme invalides lorsqu'elles représentent bien une mesure.

## Séries détaillées dans un champ répété

Les services `heart_rate_daily`, `stress_daily`, `body_battery_daily`,
`respiration_daily`, `spo2_daily`, `hrv_daily` et `sleep_daily` exposent une
colonne **`timeseries ARRAY<STRUCT<…>>`**. Le grain de la table reste le jour
et le profil source, ou la nuit/le compte pour le sommeil. Les autres familles
ne reçoivent pas une courbe artificielle à partir de leurs résumés.

Chaque élément contient notamment :

- `sample_key`, `sample_index` et `source_array` pour identifier le point source ;
- `metric_name` et `metric_unit` pour distinguer les signaux ;
- `observed_at_utc`, `observed_at_local`, `end_at_utc` et `timestamp_role` ;
- `measurement_value`, la valeur qualifiée, et `source_value` avant masquage ;
- `reading_confidence` typé `INT64` pour les points SpO₂ quotidiens et nocturnes ;
- `quality_status` et `quality_issues`, propres au point ;
- les timestamps d'origine et `source_sample`, le petit objet source sérialisé,
  qui conserve notamment les autres attributs de statut non interprétés.

Dans `spo2_daily.timeseries` et `sleep_daily.timeseries`, `reading_confidence`
est repris directement de la lecture Garmin, sans conversion ni seuil ajouté.
Il reste NULL pour les moyennes horaires SpO₂ et les autres métriques nocturnes
qui ne fournissent pas cet attribut. Sa présence ne transforme pas un point
manquant en mesure valide et sa valeur n'est pas interprétée comme un pourcentage.

Les séries sont ordonnées par instant UTC, puis métrique, tableau source et
index. Les timestamps invalides restent présents à la fin du tableau. Un point
sans valeur reste présent avec `measurement_value = NULL` pour représenter un
trou ; une série absente donne `[]`. Aucun point n'est interpolé ou généré à
partir de la moyenne quotidienne.

Le timestamp n'est pas une clé : deux observations au même instant sont
conservées lorsqu'elles ont des offsets ou des métriques différents. L'identité
d'un point repose sur le parent source, le tableau, la métrique et l'offset.
Les sept vues `hub_health_stg__*_samples` préparent ces points ; elles ne
constituent pas une seconde copie matérialisée.

Le sommeil conserve les intervalles explicites de niveaux d'activité,
mouvements et perturbations respiratoires, ainsi que les points FC, HRV,
stress, Body Battery, SpO₂ et respiration. Les codes d'activité Garmin ne sont
pas traduits arbitrairement en stades de sommeil. Les moyennes respiratoires
horodatées en fin de fenêtre portent `timestamp_role = 'end'`, sans début de
fenêtre inventé.

Les séries brutes et agrégées restent distinctes par `metric_name` et
`source_array` : ne pas les additionner comme des mesures indépendantes du
même phénomène. Le service stress comprend aussi les points Body Battery de
son propre endpoint ; le service Body Battery comprend les points stress du
sien. Les anciens points Body Battery ne sont utilisés que si le tableau moderne
de ce même enregistrement est vide.

La qualification quotidienne porte sur les résumés ; celle des points doit se
lire dans `timeseries`. Une synthèse valide ne garantit pas que chaque point de
sa courbe soit valide.

Exemple d'accès aux points FC sans créer une table supplémentaire :

```sql
SELECT
    d.health_date,
    p.observed_at_utc,
    p.measurement_value AS heart_rate_bpm,
    p.quality_status
FROM `ela-dp-dev.hub_health_svc.heart_rate_daily` AS d,
    UNNEST(d.timeseries) AS p
WHERE d.health_date = DATE '2026-10-18'
    AND p.metric_name = 'heart_rate'
ORDER BY p.observed_at_utc, p.sample_index
```

Construction de cette extension en dev :

```sh
dbt run --profiles-dir . --target dev --select tag:health_timeseries
```

Cette sélection reconstruit sept vues de points et les sept services concernés,
en supposant leurs préparations quotidiennes et le lake déjà construits. Sur
un environnement neuf, construire d'abord le domaine Health complet.

Lecture de contrôle après construction du 18/10/2026 : 1 563 647 éléments répétés,
répartis comme suit. Le nombre de lignes quotidiennes et le nombre de points
sont identiques entre préparation et service pour chaque famille.

| Service | Lignes quotidiennes | Éléments répétés | Jours sans points |
|---|---:|---:|---:|
| Body Battery | 1 083 | 171 720 | 1 |
| Fréquence cardiaque | 1 083 | 220 547 | 763 |
| HRV | 980 | 12 489 | 837 |
| Respiration | 1 083 | 249 927 | 763 |
| Stress | 1 083 | 302 924 | 763 |
| SpO₂ | 1 083 | 75 128 | 766 |
| Sommeil | 1 110 | 530 912 | 824 |

La couverture dépend des tableaux présents dans les services du lake ; un
historique de résumés ne prouve pas la présence de points détaillés. En
particulier, les nouveaux résumés HRV bronze peuvent avoir un tableau vide :
le hub ne recrée pas des anciennes lectures absentes de cet état source.
Les 14 modèles de l'extension ont été matérialisés avec succès en dev, et le
lint SQLFluff ainsi que le chargement des schémas YAML ont été vérifiés. Aucun
test dbt n'a été ajouté ou lancé et la production reste inchangée.

## Cas de source pris en charge

- Les timestamps numériques du sommeil et du poids sont des millisecondes Unix
  dans les données inspectées. La macro `garmin_health_timestamp` accepte aussi
  les chaînes temporelles ISO ; elle n'invente pas une unité pour d'autres formats.
- `recoveryTime` est en **minutes**. Le hub expose la valeur source, la durée
  effective en minutes et sa conversion en heures. `REACHED_ZERO` donne une durée
  effective de zéro tout en conservant la valeur source. Référence :
  [client python-garminconnect, TrainingReadiness](https://github.com/cyberjunky/python-garminconnect/blob/master/garminconnect/typed.py).
- Body Battery accepte les anciens points `{timestamp, value}` et le nouveau
  format déjà parsé par le lake. Les extrema utilisent les valeurs disponibles.
  Les points sans valeur sont comptés dans `missing_sample_count` et entraînent
  une qualification partielle ; ils ne sont pas assimilés à une valeur nulle
  de batterie. Les valeurs renseignées invalides sont comptées séparément.
- Les sources poids et composition partagent des identifiants de mesure. Dans
  le dev inspecté, les 235 identifiants communs ont tous le même timestamp.
  La réconciliation utilise cet identifiant, puis timestamp/date en repli.
  La date d'ingestion la plus récente sélectionne le socle ; les champs de
  composition manquants peuvent venir de l'autre observation de la même mesure.
- Les réponses poids/composition sans identifiant, timestamp ni poids sont des
  enveloppes sans mesure individuelle. Elles restent consultables dans
  `hub_health_stg__body_measurement_rejects`, avec le motif `not_a_measurement`
  et le payload source complet. Elles ne gonflent pas le nombre de mesures.
- La readiness bronze a déjà réduit les observations à une ligne par jour/profil.
  Le hub ne prétend pas reconstituer les snapshots intrajournaliers perdus en amont.
- Les champs explicitement relatifs à la nuit suivante sont exclus des services
  respiration et SpO₂. Le domaine ne produit pas de contexte prédictif avant activité.

## Consommateurs raccordés

Les six produits santé Data4Agent lisent désormais les services Health : sommeil,
HRV, repos cardiaque, stress, readiness et statut d'entraînement. Leurs colonnes
restent explicites. Les heures locales historiques représentées par des `TIMESTAMP`
et les chaînes d'epoch du sommeil sont conservées dans ces contrats gold.
La valeur `recovery_time_hours` est corrigée et devient une vraie durée en heures,
potentiellement fractionnaire.

La homepage lit Health pour ses blocs santé et état d'entraînement. Le master
course utilise les services sommeil, readiness, statut, VO₂max et mesures
corporelles. Pour le poids, il sélectionne la dernière mesure de chaque jour avant
d'établir les intervalles de validité, ce qui évite de multiplier les activités
ayant plusieurs pesées le même jour. Ce contexte est descriptif à la journée,
pas une garantie de disponibilité avant le départ de la course.

Il reste trois produits en accès direct au lake Garmin : Data4Agent activités,
Data4Agent intervalles et la liste d'activités Webapp. Ils appartiennent au
chantier du socle Activities tous sports.

## Exécution et promotion

La construction de développement utilise `dbt run`, sans lancement ni ajout de
tests, conformément au choix de cette étape. SQLFluff, compilation/parse et
chargement YAML complètent la vérification de matérialisation.

Les 27 modèles Health initiaux, le master Activities et les neuf produits raccordés ont
été construits en dev. Après correction des deux incompatibilités SQL rencontrées
pendant le développement, les relances ciblées sont terminées avec succès.
La dernière relance comprend les deux familles ajustées, la vue de rejets, le
master et son produit `activity_detail` : huit modèles réussis.
Le service corporel contient 355 mesures ; les 64 réponses sans mesure sont
séparées. Les observations partielles ou invalides des autres familles restent
signalées, notamment 14 lignes HRV hors domaine lors de la lecture de contrôle.

Construire le domaine Health avant ses consommateurs, ou les sélectionner ensemble :

```sh
dbt run --profiles-dir . --target dev --select path:models/hub/health
dbt run --profiles-dir . --target dev --select path:models/hub/activities
```

Reconstruire ensuite les produits raccordés. Pour le premier passage, le produit
`pct_webapp_activity_focus__activity_detail` requiert un `--full-refresh` ciblé :
la durée de récupération du master devient un nombre d'heures fractionnaire.
Les six produits Data4Agent santé et les deux blocs homepage sont des tables
reconstruites à chaque exécution.

Les nouveaux datasets ont été créés en dev par dbt. La production n'est pas
modifiée. Avant promotion, provisionner `hub_health_stg` et `hub_health_svc` et
leurs accès dans l'infrastructure gérée, puis vérifier que la sélection des jobs
inclut Health avant Activities et Product. Une sélection limitée au seul chemin
Activities ne construit pas automatiquement les nouveaux parents Health.

Les filtres incrémentaux du bronze et de certains produits Activities restent
hors de cette intervention. Le fonctionnement multi-compte exige une résolution
d'identité et une adaptation des jointures du master, qui restent actuellement
prévues pour le compte Garmin de la plateforme.
