# ELA Data Platform — dbt

Transformations BigQuery de la plateforme ELA : données Garmin et Spotify,
domaines réutilisables du hub, puis tables destinées à la Webapp et aux agents.
L'ingestion et l'infrastructure sont gérées dans les dépôts voisins
`ela-dp-ingestion` et `ela-infra-as-code`.

## Architecture et domaines

```text
Ingestion → Lake normalized → Lake staging → Lake service
                                                ↓
                                      Hub staging → Hub service
                                                        ↓
                                               Product → API / applications
```

| Couche | Rôle | Datasets BigQuery |
|---|---|---|
| Lake normalized | Tables brutes alimentées par l'ingestion, déclarées comme sources dbt | `dp_lake_garmin_<env>`, `dp_lake_spotify_<env>` |
| Lake staging | Parsing, préparation et sélection des enregistrements canoniques | `lake_garmin_stg`, `lake_spotify_stg`, `lake_spotify_legacy_stg` |
| Lake service | Persistance des états canoniques ; réconciliation des corrections historiques | `lake_garmin_svc`, `lake_spotify_svc`, `lake_spotify_legacy_svc` |
| Hub | Entités, mesures de domaine, unités, qualification et provenance | `hub_music_*`, `hub_health_*`, `hub_activities_*`, `hub_utils_stg` |
| Product | Projections, calculs et formats adaptés à un consommateur | `product_webapp`, `product_data4agent` |

Les schémas `*_stg` contiennent les préparations ; `*_svc` expose les interfaces
publiques du domaine. Le type de matérialisation dépend du modèle : certaines
tables sont reconstruites, d'autres utilisent un merge incrémental. Un modèle
incrémental ne garantit pas à lui seul une lecture limitée aux nouvelles lignes.

| Domaine | Interfaces principales | Grain et contenu |
|---|---|---|
| Music | `fact_played`, `ref_track`, `ref_album`, `ref_artist`, `bridge_track_artist`, `bridge_album_artist` | Écoutes, catalogue et relations ordonnées entre entités |
| Health | `sleep_daily`, `hrv_daily`, `rhr_daily`, `wellness_daily`, `body_measurement` et autres familles santé | Nuit, jour ou mesure individuelle selon le service ; courbes imbriquées lorsqu'elles existent |
| Activities | `fact_activity`, `activity_recording`, `activity_splits`, `activity_zones`, `activity_exercise_sets`, `activity_weather`, `master_activities` | Activités tous sports, mesures qualifiées et détails en `STRUCT` / `ARRAY<STRUCT>` |
| Utils | `ref_calendar` | Calendrier réutilisable |

Le hub conserve l'historique disponible. Les filtres de période et les libellés
de présentation appartiennent aux produits. Les conventions détaillées sont
dans [HUB_ACTIVITIES.md](HUB_ACTIVITIES.md) et [HUB_HEALTH.md](HUB_HEALTH.md).

## Interfaces à utiliser

- Pour construire un produit Activities, partir de
  `hub_activities_svc.master_activities`, ou d'un service spécialisé. Le master
  garde une ligne par activité ; les points, tours, zones et séries sont des
  tableaux structurés. `master_running_activities` maintient l'ancien contrat
  pour les trois types de course historiques.
- Pour Health et Music, choisir le service du grain concerné. Les identifiants
  Garmin de profil, d'utilisateur et de compte ne sont pas interchangeables.
  Le contexte santé associé à une activité est descriptif ; il ne garantit pas
  une information connue avant le départ.
- Pour une application ou un agent, utiliser les produits adaptés : tables
  `activities` / `activity_intervals` et familles santé dans `product_data4agent`,
  ou familles `homepage__*`, `activities__*`, `activity_focus__*`, `artist__*` et
  `ranking__*` dans `product_webapp`.

Les descriptions de colonnes, grains et unités sont définies dans les
`schema.yml` proches des modèles. Elles constituent la documentation du contrat ;
elles ne signifient pas que tous les modèles ont un contrat de types imposé par dbt.
Une mesure manquante ou invalide n'est pas un zéro. Lire les champs de qualité,
de disponibilité et de provenance lorsque le domaine les expose.

Les noms de fichiers servent aux `ref()` et aux sélecteurs dbt. Les alias SQL
sont définis par [generate_alias_name.sql](macros/generate_alias_name.sql) :

```text
hub_activities_svc__master_activities       → hub_activities_svc.master_activities
pct_data4agent__activities                 → product_data4agent.activities
pct_webapp_activity_focus__activity_detail → product_webapp.activity_focus__activity_detail
```

## Environnements et installation

| Cible dbt | Projet GCP | Région BigQuery | Branche déployée |
|---|---|---|---|
| `dev` | `ela-dp-dev` | `EU` | `develop` |
| `prd` | `ela-dp-prd` | `EU` | `main` |

Les noms des datasets hub/product sont identiques dans les deux projets ; le
projet GCP assure la séparation. Changer seulement `target.schema` ne crée pas
une cible isolée pour les modèles qui déclarent un schéma personnalisé : voir
[generate_schema_name.sql](macros/generate_schema_name.sql).

Pour les commandes dbt locales : Docker avec son moteur démarré, Git, Bash et
`make`. Google Cloud CLI et les accès BigQuery de dev sont nécessaires pour les
commandes qui interrogent l'entrepôt. Python local n'est pas nécessaire pour
exécuter dbt dans l'image.

```sh
make docker-build       # Construit ela-dp-dbt:local depuis le Dockerfile
make deps               # Installe les packages dbt dans le workspace monté
make parse              # Parsing hors ligne, dans l'image
```

Le code local est monté dans `/dbt` : il n'est pas nécessaire de reconstruire
l'image après chaque modification SQL. Les packages, logs et artefacts sont
écrits dans `dbt_packages/`, `logs/` et `target/` du workspace, avec l'UID/GID
local. Reconstruire l'image lorsque ses dépendances changent et relancer
`make deps` après une modification des packages dbt.

Pour utiliser une image publiée, configurer l'accès au registre puis préciser
son tag ou, pour une version précise, son digest :

```sh
gcloud auth configure-docker europe-docker.pkg.dev
make docker-pull IMAGE=europe-docker.pkg.dev/ela-dp-dev/dbt/ela-dp-dbt:dev
make parse IMAGE=europe-docker.pkg.dev/ela-dp-dev/dbt/ela-dp-dbt:dev
```

`IMAGE` s'applique à toutes les commandes Docker. `PLATFORM=linux/amd64` peut
être précisé pour une image publiée uniquement pour cette architecture,
notamment sur un Mac Apple Silicon. Le build local utilise par défaut
l'architecture de la machine.

Le profil suivi dans [profiles.yml](profiles.yml) définit déjà les projets ELA.
La cible locale par défaut est `dev`. Le développement utilise l'authentification
OAuth locale :

```sh
gcloud auth application-default login
make debug
```

Le wrapper monte uniquement le fichier ADC en lecture seule pour les commandes
qui ont besoin de GCP. Il utilise `ADC_FILE`, puis `GOOGLE_APPLICATION_CREDENTIALS`,
sinon le fichier `application_default_credentials.json` du répertoire gcloud.
Les credentials ne sont pas copiés dans l'image. `parse`, `ls`, `deps`, la version
et le serveur de documentation n'en ont pas besoin.

Pour les outils de contribution, ajouter Python 3.12 et
[uv](https://docs.astral.sh/uv/getting-started/installation/), puis lancer
`make setup`. Cette commande installe les dépendances verrouillées, les packages
dbt et les hooks Git. Le lint et le parsing de CI restent indépendants de Docker
et de l'authentification GCP.

## Commandes de développement

```sh
make help          # Catalogue des commandes
make check         # Lint SQL + YAML + parsing dbt, sans écrire de tables
make hooks         # Tous les hooks sur les fichiers suivis par Git
make fix-sql       # Corrections de formatage SQL, lancées explicitement
```

SQLFluff utilise **une version verrouillée dans `uv.lock`**, le dialecte BigQuery
et le templater Jinja de [.sqlfluff](.sqlfluff). La commande locale, le hook et
la CI passent tous par [scripts/sqlfluff.sh](scripts/sqlfluff.sh). Le lint reste
hors ligne ; la compilation dbt et l'exécution BigQuery se vérifient séparément.
Le hook contrôle les fichiers modifiés sans les reformater automatiquement.

Pour cibler un fichier avec les mêmes règles :

```sh
./scripts/sqlfluff.sh lint models/hub/activities/staging/hub_activities_stg__fact_activity.sql
./scripts/sqlfluff.sh fix models/hub/activities/staging/hub_activities_stg__fact_activity.sql
```

Pour vérifier puis construire une sélection en dev avec l'image :

```sh
make ls SELECT=tag:activities
make compile SELECT=hub_activities_svc__master_activities
make run SELECT=+hub_activities_svc__master_activities
make test SELECT=tag:activities
make build SELECT=tag:activities          # dbt build : modèles et tests
make run SELECT=tag:activities ARGS='--full-refresh'
make freshness SELECT=source:garmin
make dbt ARGS='ls --select tag:health'    # Accès libre à la CLI dbt de l'image
```

`make docker-build` construit l'image ; `make build` exécute **dbt build**.
`TARGET=dev` est la valeur par défaut ; `TARGET=prd` désigne le projet de
production et ne doit être utilisé que pour une action explicitement autorisée.
`make run` sans `SELECT` conserve le comportement de dbt : toute la sélection
par défaut du projet est exécutée.

`make parse-local` utilise `uv.lock` et constitue l'étape de parsing de
`make check` et de la CI. La CI Docker teste également les vraies commandes
`make deps` et `make parse` avec l'image qu'elle vient de construire.

`+modele` inclut les parents ; `modele+` inclut les descendants. Les tags
`garmin` et `spotify` suivent la source ; `activities`, `health`, `webapp` et
`data4agent` permettent des sélections plus précises. Vérifier la sélection
avec `dbt ls` avant une reconstruction large. Les modèles de préparation et
les services amont doivent exister si leurs parents ne sont pas sélectionnés.

Les tests unitaires SQL et les assertions de données figurent dans `tests/` et
dans les YAML des modèles. Une qualification `partial` n'est pas nécessairement
un échec de test : certaines mesures sont facultatives ou leur unité reste à confirmer.

## Catalogue et documentation dbt

```sh
make docs          # Génère le catalogue dev dans target/ ; lit les métadonnées BigQuery
make docs-serve    # Sert la documentation locale sur http://localhost:8080
```

Ces deux commandes utilisent la même image Docker et le workspace local.
`PORT=8081` permet de choisir un autre port hôte. Le port publié est limité
à `127.0.0.1` ; le serveur n'ouvre pas automatiquement de navigateur.

Le catalogue donne accès aux modèles, colonnes documentées et dépendances. Les
artefacts générés dans `target/` ne sont pas versionnés. Le site n'est pas
publié automatiquement ; les commandes ci-dessus servent la documentation locale.
Voir aussi la [documentation officielle de dbt docs](https://docs.getdbt.com/reference/commands/cmd-docs).

| Document | Usage |
|---|---|
| [HUB_ACTIVITIES.md](HUB_ACTIVITIES.md) | Contrats imbriqués, unités, limites, consommation et migration Activities |
| [HUB_HEALTH.md](HUB_HEALTH.md) | Grains santé, qualification, séries et rapprochements temporels |
| [LATE_UPDATES.md](LATE_UPDATES.md) | Réconciliation des corrections et arrivées tardives |
| [HUB_SILVER_PROPOSAL.md](HUB_SILVER_PROPOSAL.md) | Audit et proposition historiques ; à lire avec les implémentations plus récentes |
| [HUB_SILVER_P1.md](HUB_SILVER_P1.md) | Première étape historique de fiabilisation du hub |

## Contribution et promotion

1. Créer une branche de travail depuis `develop` et limiter les changements à un
   périmètre explicite. Ajouter ou mettre à jour les descriptions et les contrôles
   pertinents avec le modèle concerné.
2. Exécuter les contrôles locaux, puis construire et tester les modèles concernés
   en dev lorsque le changement modifie les transformations.
3. Ouvrir une PR vers `develop`. Les
   [contrôles de PR](.github/workflows/pr-checks.yml) exécutent les mêmes commandes
   de lint et de parsing que le local. La
   [CI Docker](.github/workflows/docker-ci.yml) construit l'image et vérifie son
   parsing pour les changements qui concernent l'image ou les transformations.
4. Après validation, promouvoir `develop` vers `main` par PR. Aucun push direct
   ni commit de travail sur `main`.
5. Le [workflow CD](.github/workflows/docker-cd.yml) publie l'image dans le projet
   de la branche et épingle les jobs Cloud Run `ela-dp-dbt-*` à son digest.
   **Il ne lance pas la reconstruction ni les tests de données.** Après un
   déploiement de modèles, exécuter le job concerné, vérifier son résultat et
   lancer les tests correspondants avant de considérer la promotion terminée.

Exemple d'exécution de production, à utiliser pour une promotion explicitement
autorisée :

```sh
gcloud run jobs execute ela-dp-dbt-garmin \
  --project=ela-dp-prd --region=europe-west1 --wait

gcloud run jobs execute ela-dp-dbt-garmin \
  --project=ela-dp-prd --region=europe-west1 \
  --args=test,--target,prd,--select,tag:activities --wait
```

Les arguments d'exécution des tests sont temporaires ; ils ne remplacent pas
les arguments du job quotidien. Les sélections Spotify comportent des exclusions
spécifiques au flux d'écoutes fréquent : consulter le workflow CD avant de les
modifier. L'état de fraîcheur et les résultats d'exécution doivent être contrôlés
dans l'environnement concerné ; une CI verte ne démontre pas leur actualité.

Les produits doivent consommer le hub, ou le lake en dernier recours. Une logique
partagée doit remonter dans le hub plutôt que créer une nouvelle dépendance
Product → Product. Les exceptions héritées ne constituent pas un modèle à reproduire.
