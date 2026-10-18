# Hub / Silver : audit et proposition

Audit du 18 octobre 2026. Périmètre : code local et lectures agrégées sur
`ela-dp-prd`, région EU. Aucun modèle reconstruit et aucune donnée modifiée.

## Recommandation

Faire du hub le contrat de confiance entre les sources et les produits : des
entités et événements au grain explicite, des règles communes de qualification,
des référentiels couvrant les faits, et des anomalies visibles et traitables.
Conserver l'organisation par domaine et les sous-couches `stg` / `svc`.

Le premier chantier devrait fermer les défauts de fiabilité existants, puis
ouvrir deux socles réutilisables : activités tous sports et santé quotidienne.
Éviter de recopier les 29 familles Garmin en silver sans besoin identifié.

## État observé

Le manifest local généré le 18/10/2026 décrit 147 modèles actifs :

| Couche | Modèles actifs | Tests de données rattachés à la couche | Observation |
|---|---:|---:|---|
| Lake | 92 | 0 | Parsing et déduplication ; la qualité effective reste à mesurer par famille |
| Hub | 19 | 28 | Les tests couvrent 6 modèles Music ; aucun test rattaché au master Activities |
| Product | 36 | 93 | 11 modèles lisent directement le lake |

Le hub comprend 10 vues Music de préparation, 6 tables Music de service,
une vue et une table de course, et une vue de périodes. Les modèles playlists
sont désactivés. Les worklists d'enrichissement ont une fonction opérationnelle ;
elles ne constituent pas des interfaces silver pour les produits.

Les 8 modèles Data4Agent consomment le lake, ainsi que `homepage__health`,
`homepage__training_state` et `activities__activity_list`. Le silver ne couvre
donc pas encore les besoins déjà présents dans le gold.

Sources de structure : [configuration](dbt_project.yml),
[hub](models/hub), [produits](models/product), manifest local.
Le README est encore celui du projet dbt initial ; `CLAUDE.md` et
`REFACTOR_PLAN.md` contiennent des descriptions historiques du naming.
En production, les relations `hub_music_svc` / `hub_activities_svc` existent
ainsi que les anciennes relations `dp_hub_prd`. Les consommateurs externes
n'ont pas été inventoriés : ne pas déduire de cette coexistence lequel ils lisent.

## Mesures en production

Contrôles exécutés sur l'ensemble des tables concernées, sans filtre temporel.
Les pourcentages utilisent le dénominateur de chaque ligne, pas les seules écoutes.

| Contrôle | Résultat | Conséquence |
|---|---:|---|
| Clés nulles / doublons : écoutes, titres, albums, artistes, courses | 0 sur chaque table | Socle de clés sain pour ces cinq tables |
| Écoutes sans titre | 0 / 14 715 | Couverture des écoutes assurée sur ce contrôle |
| Titres sans album correspondant | 11 / 20 264 (0,054 %) | Référentiel catalogue incomplet ; aucune écoute sans album dans le fait |
| Relations titre–artiste sans artiste référencé | 3 335 / 30 072 (11,09 %) | 1 610 artistes distincts absents ; 7 écoutes concernées par au moins une relation orpheline |
| Relations album–artiste sans artiste référencé | 1 / 3 275 (0,031 %) | Couverture incomplète du bridge |
| Titres ayant plusieurs artistes en position 0 | 3 / 20 264 | Risque de multiplication des lignes au rattachement de l'artiste principal |
| Albums ayant plusieurs artistes en position 0 | 5 / 2 567 | Même risque pour les classements et fiches albums |
| Titres sans artiste en position 0 | 0 / 20 264 | Le problème mesuré est la multiplicité |
| Albums sans artiste en position 0 | 0 / 2 567 | Même observation |
| Durée de titre absente ou non positive | 0 / 20 264 | Pas d'anomalie sur cette règle |
| Courses avec distance ou durée négative | 0 / 371 | Ne démontre pas la cohérence de toutes les mesures |
| Courses sans timeseries | 0 / 371 | Complétude structurelle sur ce contrôle |
| Détails Garmin ingérés après le timestamp porté par le master | 362 / 371 (97,57 %) | Signal de risque, pas preuve de contenu périmé |

Vérification complémentaire Garmin sur 371 activités communes entre vue et table :
aucune différence pour sommeil, météo, zones FC, contexte athlétique et route,
ni pour le nombre de points de timeseries. Le contenu de chaque point et les
autres champs n'ont pas été comparés. Le watermark fragile n'est donc pas
présenté comme un incident de fraîcheur effectivement constaté.

Les maxima `_ingested_at` sont le 18/10 pour écoutes, titres et artistes,
le 17/10 pour albums, et le 14/10 pour courses (UTC). Une absence de nouvelle
course ne suffit pas à conclure à une panne. Ces valeurs ne donnent pas l'heure
de réussite des jobs et ne prouvent pas la complétude de l'ingestion.

Requêtes reproductibles : [profil et intégrité](analyses/audit_hub_silver.sql),
[impact et comparaison Garmin](analyses/audit_hub_silver_followup.sql),
[types](analyses/audit_hub_silver_schema.sql).
Jobs BigQuery : `bqjob_r635275f27be5ad89_000001a14f51486f_1`
et `bqjob_r656192b0a0cbfe70_000001a14f5301ab_1` dans `ela-dp-prd`, EU.
Chaque requête a été limitée à 1 Gio facturable ; aucun `dbt build` ou test
matérialisant ses résultats n'a été exécuté en production.

## Défauts et risques à traiter

### P1 — Compléter et stabiliser les référentiels Music

**Constat confirmé, confiance élevée.** Le catalogue contient des orphelins et
plusieurs artistes principaux pour certaines entités. La portée sur les écoutes
est faible à cet instant, mais le contrat d'intégrité n'est pas tenu.

Dans `hub_music_stg__ref_artist`, les artistes viennent des détails, du top et
des écoutes ; ceux présents uniquement dans les pistes d'albums n'y entrent pas.
Cette asymétrie avec `bridge_track_artist` explique une cause plausible des trous,
sans attribuer les 1 610 artistes à cette seule cause avant réconciliation.
Les référentiels titres et albums omettent aussi certaines familles disponibles,
notamment les éléments sauvegardés dans leurs unions de métadonnées.

Construire d'abord l'univers des clés depuis tous les faits et bridges du domaine,
puis enrichir depuis les sources disponibles. Un album ou artiste connu mais
non enrichi doit exister comme référence minimale avec `enrichment_status`.
Une sentinelle d'échec API ne doit pas devenir un enregistrement prétendument complet.

Les bridges dédupliquent chaque couple entité–artiste indépendamment et les merges
ne suppriment pas les anciennes relations. Deux versions d'une liste d'artistes
peuvent donc être mélangées. Choisir une liste canonique entière par titre/album,
selon une priorité et un départage déterministes, puis remplacer l'ensemble
des relations de l'entité modifiée. Une reconstruction complète des bridges
est une option simple à évaluer sur les volumes actuels. Un `ROW_NUMBER` par
position masquant arbitrairement un conflit ne constitue pas sa résolution.

### P1 — Corriger le contrat de type Activities

**Défaut confirmé en production, confiance élevée.**
`athletic_context.training_status_code` est un `STRUCT` contenant toute la ligne
source, au lieu du code numérique attendu. Dans la CTE `training_status`,
`training_status AS code` entre en collision avec le nom implicite de la relation.
Qualifier la colonne par un alias explicite et vérifier le schéma résultant.
Prévoir la migration du type physique et vérifier ses consommateurs en dev
avant promotion ; un merge ordinaire ne doit pas être supposé suffisant.

### P1 — Rendre les mises à jour rejouables

**Risque prouvé par le code, impact non démontré sur tous les champs.**
Le master Garmin ne propage que `a._ingested_at`, même si détails, splits,
sommeil ou contexte changent. `details_ingested_at` est extrait mais inutilisé.
De plus, `route` est sélectionné mais absent de `merge_update_columns`.

Les services Hub filtrent avec `_ingested_at > MAX(cible._ingested_at)` :
une cible vide donne un maximum NULL ; les timestamps égaux ou des arrivées
hors ordre peuvent être ignorés. Pour Music, la date de la ligne retenue selon
la priorité de source n'est pas nécessairement la date de changement de l'entité.
Ce motif existe aussi dans le bronze et mérite un correctif ciblé si nécessaire.

Pour démarrer, privilégier la reconstruction des petites dimensions et des
bridges si la mesure de coût le permet. Pour les faits volumineux, identifier
les clés affectées depuis toutes leurs dépendances, recalculer ces clés et merger.
Une mise à jour d'une mesure valable jusqu'à la suivante (poids, VO2max) doit
invalider l'intervalle d'activités concerné, pas seulement le jour de mesure.
Ajouter un chevauchement temporel aux watermarks pour les retards ordinaires,
et un mécanisme explicite de rejeu historique pour ceux hors fenêtre.

Valider l'équivalence incrémental / reconstruction complète sur un jeu dev,
y compris correction ancienne, égalité de timestamps et cible vide.

### P1 — Faire respecter la qualité à l'exécution

**Constats de dépôt, confiance élevée ; orchestration externe non auditée.**
Le Dockerfile ne copie pas `tests/` : les deux assertions SQL sur les artistes
principaux ne sont pas embarquées dans l'image. Les tests génériques YAML restent
présents via `models/`. Les assertions actuelles détectent plusieurs artistes
principaux, mais pas leur absence.

La CI fait lint, parse et build d'image ; elle ne démontre pas la qualité réelle
des données. L'entrypoint exécute `dbt run` par défaut, sans tests de données.
Les arguments effectifs des jobs Cloud Run et leurs dépendances restent à vérifier.
La fraîcheur des sources est déjà déclarée (24 h / 48 h) ; vérifier son exécution
et adapter les règles à la cadence des flux, en particulier les sources inactives.

Embarquer les assertions, ajouter les tests de grain et de domaine manquants,
et imposer une étape de validation hub avant reconstruction product. `dbt build`
ordonne modèles et tests et peut bloquer des descendants sur échec ; les règles
pour les tests à plusieurs parents nécessitent une sélection et des dépendances
vérifiées. Pour deux jobs séparés, faire dépendre le job product de la réussite
du job hub et de ses contrôles critiques. Un test après écriture ne garantit
pas, à lui seul, une publication atomique ni la conservation du dernier silver sain.
Pour ce besoin, construire dans une cible de validation puis publier après succès.
Voir la [documentation dbt build](https://docs.getdbt.com/reference/commands/build).

## Contrat cible du silver

| Responsabilité | Garantie attendue |
|---|---|
| Grain et identité | Une clé documentée et testée ; clé composite pour faits et bridges |
| Types et unités | Noms stables, unités explicites, dates typées, tableaux typés ; JSON résiduel réservé aux attributs réellement ouverts |
| Temps | Instants en UTC ; heure locale sans faux UTC ; date métier et fuseau documentés |
| Réconciliation | Priorité par attribut ou groupe cohérent d'attributs ; départage stable ; provenance conservée |
| Intégrité | Références minimales pour entités connues ; clés nulles et liens invalides signalés |
| Qualification | Absence, zéro, non-applicable et valeur invalide distingués ; disponibilité par famille |
| Historique | Dimensions courantes en première intention ; historique uniquement pour attributs dont les usages exigent le passé |
| Exploitabilité | Rejeu, contrôles, observabilité et politique de publication documentés |

Pour chaque service : description du grain, clé, colonnes et types, unités,
colonnes obligatoires, règles stables, stratégie de mise à jour et consommateurs.
Les contrats dbt de colonnes/types peuvent figer l'interface ; ils ne remplacent
pas les tests d'unicité et d'intégrité des données.
Voir les [contrats dbt](https://docs.getdbt.com/docs/mesh/govern/model-contracts).

Prévoir `source_system`, `source_record_id` lorsque pertinent,
`source_ingested_at`, `silver_processed_at` et une provenance des enrichissements.
Le timestamp de traitement ne doit pas devenir artificiellement la date de
changement métier de toutes les lignes à chaque run.

Un statut de ligne et des codes d'anomalie doivent rester simples. Exemple :
`valid`, `partial`, `invalid` pour la qualification, et un statut séparé
`pending`, `complete`, `unavailable` pour l'enrichissement. Un enrichissement
facultatif absent ne doit pas rendre tout le fait inexploitable.

Conserver les rejets avec clé source et motif, puis publier des mesures par modèle,
date et règle : lignes évaluées, lignes affectées, taux, fraîcheur et couverture.
Échec bloquant pour clé invalide, grain dupliqué et type incompatible ; avertissement
pour enrichissement optionnel absent ou valeur inhabituelle à examiner.
Éviter de supprimer silencieusement une activité parce qu'un capteur manque.

## Modèles à construire

Noms proposés, à conserver dans la convention actuelle. Les grains ci-dessous
supposent un seul compte source aujourd'hui ; expliciter cette limite. Si plusieurs
utilisateurs sont attendus, inclure leur identité dans les clés et dans les
jointures avant de les agréger au jour. Garmin distingue déjà l'utilisateur dans
la clé bronze de certains jeux, alors que le master joint essentiellement par date.

| Domaine / service proposé | Grain | Usage |
|---|---|---|
| Music : services existants corrigés | Écoute `(played_at, track_id)`, référentiels par ID, bridges par couple | Consolider le socle déjà utilisé |
| Activities : `hub_activities_svc__fact_activity` | Une activité, tous sports | Socle commun Data4Agent, listes et course ; identité, UTC, mesures et qualification |
| Activities : `hub_activities_svc__activity_lap` | Une activité et un index de lap | Intervalles et découpage ; types de splits séparés si grains différents |
| Activities : `hub_activities_svc__activity_sample` | Une activité et un index d'échantillon source | Timeseries ; timestamp documenté et contrôlé, sans supposer son unicité |
| Activities : `hub_activities_svc__activity_hr_zone` | Une activité et un numéro de zone | Contrôle et réutilisation des zones |
| Health : `hub_health_svc__sleep_daily` | Une date de sommeil par utilisateur | Sommeil et durées, timestamps typés, convention de nuit explicite |
| Health : `hub_health_svc__hrv_daily`, `rhr_daily`, `stress_daily` | Une date et un utilisateur par famille | Remplacer les lectures lake de Data4Agent |
| Health : `hub_health_svc__training_readiness_daily`, `training_status_daily` | Une date et un utilisateur selon la règle de sélection source | État d'entraînement qualifié et réutilisable |
| Health : `hub_health_svc__vo2max_daily`, `weight_measurement` | Date/mesure et utilisateur selon le grain réel | Valeur, date de mesure et validité explicites |

Créer les paires `stg` / `svc` uniquement lorsque la séparation préparation /
publication apporte quelque chose. Confirmer les grains réels avant de figer
les contrats, notamment poids, mesures multi-appareils et readiness intrajournalière.
Les modèles non utilisés par le gold actuel peuvent attendre.

Conserver `master_running_activities` comme assemblage compatible alimenté par
ces briques. Les structures BigQuery sont utiles ; l'objectif du découpage est
de contrôler séparément les grains et de réutiliser les mesures, pas d'interdire
les structures imbriquées. Ajouter `has_timeseries`, `has_hr`, `has_gps` et les
motifs d'absence pertinents à l'assemblage.

Dans Health, garder les familles indépendantes avant de créer éventuellement
un résumé utilisateur/jour : une table géante ferait perdre la provenance,
le grain et les raisons de nullité. Le rapprochement temporel pour une course
doit distinguer contexte descriptif du jour et information disponible avant le
départ, afin d'éviter d'utiliser des données postérieures pour une recommandation.

Vérifier les unités Garmin depuis les données/source avant de les contractualiser,
en particulier `recoverytime AS recovery_time_hours`, cadence et longueur de foulée.
Vérifier aussi le `TIMESTAMP` nommé `start_time_local` observé en prod : un horaire
local doit être interprété avec son fuseau réel, pas converti une seconde fois.
Pour les albums, conserver la précision année/mois/jour plutôt qu'inventer une
date exacte ; pour les durées d'écoute, distinguer durée nominale du titre et
temps réellement écouté, qui n'est pas démontré par les faits actuels.

Le gold garde les fenêtres de reporting, classements, libellés, verdicts et
formats d'API. Les unités de référence, règles de qualité et rapprochements
réutilisables appartiennent au hub. Une règle métier partagée remonte au hub ;
une préférence d'affichage reste au produit.

## Livraison par lots

| Lot | Travail | Critère de sortie |
|---|---|---|
| 1 — Fiabiliser l'existant | Référentiels et bridges Music, type du code Garmin, propagation des modifications et route, tests embarqués | Orphelins résolus ou explicitement qualifiés, un artiste principal par entité applicable, type attendu ; tests dev d'idempotence, cible vide et enrichissement tardif |
| 2 — Activités communes | `fact_activity` tous sports, laps et zones ; samples si besoin immédiat ; réassembler le master | Clés/grains testés, volumes et mesures réconciliés avec le lake ; contrats product conservés ; Data4Agent Activities/Intervals et liste webapp migrés |
| 3 — Santé utilisée aujourd'hui | Sommeil, HRV, RHR, stress, readiness, statut et VO2max ; poids si nécessaire au master | Les 11 lectures directes product→lake sont supprimées ; unités, temps et sélection quotidienne documentés |
| 4 — Exploitation | Validation hub avant product, indicateurs de qualité, rejeu, documentation et contrôle de dépendances en CI | Incident de qualité détectable et bloquant au bon niveau ; aucune nouvelle dépendance product→lake sans exception documentée |

Les tests critiques arrivent dès le lot 1 et accompagnent chaque modèle ; le lot 4
complète l'exploitation et ne reporte pas les contrôles à la fin du chantier.
Chaque lot se valide en dev puis passe par le workflow PR du dépôt. Rejouer les
mesures prod après promotion et comparer les contrats gold avant/après. Le trafic
et les dépendances des anciennes relations doivent être vérifiés avant leur retrait.

## Limites de cet audit

La couverture bronze complète, les trous historiques d'ingestion, les mesures
intra-timeseries, les distributions physiologiques, les coûts réels et les
arguments d'orchestration Cloud Run n'ont pas été audités. Les 28 tests hub
n'ont pas été exécutés comme suite dbt ; seules les requêtes de lecture indiquées
ont été exécutées. Les constats SQL et les observations de schéma ont une confiance
élevée ; les explications de provenance et les unités non vérifiées restent des
hypothèses à confirmer dans les lots correspondants.
