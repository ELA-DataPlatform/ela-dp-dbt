# ela-dp-dbt

Instructions pour travailler sur les transformations BigQuery de la plateforme ELA.
Le [README](README.md) décrit l'installation, les interfaces et la promotion.
`dbt_project.yml`, les modèles actuels et les macros de naming font autorité
lorsqu'une ancienne proposition ou un ancien runbook les contredit.

## Architecture actuelle

Sources : **Garmin** (activités et santé), **Spotify** et historique Spotify legacy.

| Couche | Rôle | Schémas physiques |
|---|---|---|
| Lake normalized | Tables brutes alimentées par l'ingestion, déclarées avec `source()` | `dp_lake_<source>_<env>` |
| Lake staging / service | Parsing, sélection canonique et persistance | `lake_<source>_stg`, `lake_<source>_svc` |
| Hub staging / service | Entités et mesures réutilisables, qualification et provenance | `hub_<domaine>_stg`, `hub_<domaine>_svc` |
| Product | Contrats de consommation pour Webapp et agents | `product_webapp`, `product_data4agent` |

Les domaines du hub sont **Music**, **Health**, **Activities** et **Utils**.
Les services du lake réconcilient les corrections anciennes ; une matérialisation
incrémentale ne signifie pas que seules les nouvelles lignes sont lues.
Les services Health et Activities sont actuellement reconstruits complètement.

Les fichiers sont référencés par leur nom dbt ; leur alias BigQuery peut différer.
Exemple : `hub_activities_svc__master_activities` publie
`hub_activities_svc.master_activities`. Vérifier
[generate_alias_name.sql](macros/generate_alias_name.sql) et
[generate_schema_name.sql](macros/generate_schema_name.sql).

## Domaines et contrats à préserver

- **Music** : fait des écoutes, référentiels titres/albums/artistes et bridges
  ordonnés. Les relations et la sélection de l'artiste principal font partie
  du contrat ; préserver la réconciliation des changements de catalogue.
- **Health** : familles indépendantes au grain jour, nuit ou mesure corporelle.
  Les courbes disponibles sont des `ARRAY<STRUCT>`. Les identifiants de profil,
  d'utilisateur et de compte Garmin ne constituent pas une identité unifiée.
  Voir [HUB_HEALTH.md](HUB_HEALTH.md).
- **Activities** : `fact_activity` tous sports et services spécialisés
  `activity_recording`, `activity_splits`, `activity_zones`,
  `activity_exercise_sets`, `activity_weather`, assemblés dans `master_activities`.
  Une ligne par activité ; les points, tours, zones et séries restent imbriqués.
  `master_running_activities` conserve l'interface des trois anciens types de
  course pour les consommateurs existants. Voir [HUB_ACTIVITIES.md](HUB_ACTIVITIES.md).

Pour Activities, consommer les slots indexés du lake, jamais la représentation
historique dense qui retire les valeurs nulles. Préserver les offsets sources,
les points au même timestamp, les valeurs manquantes et les unités explicites.
Les familles répétées doivent être agrégées séparément avant jointure pour
éviter la multiplication des activités.

Conserver les valeurs sources, la provenance, la disponibilité et la qualification
à leur grain. Une donnée facultative absente ne doit pas supprimer une activité
ou devenir zéro. Le contexte Health du master est descriptif à la date locale ;
il ne garantit pas une information connue avant le départ.

## Exécution locale : utiliser Make et Docker

**Privilégier les cibles Make pour les commandes dbt locales**, plutôt qu'un
`dbt` ou `uv run dbt` lancé directement sur l'hôte. Elles exécutent dbt dans
l'image Docker avec le code local monté, `TARGET=dev` et
`IMAGE=ela-dp-dbt:local` par défaut.

```sh
make help
make docker-build
make deps
make parse
```

Docker doit être démarré. Pour les commandes qui interrogent BigQuery, utiliser
les credentials ADC existants (`gcloud auth application-default login` si nécessaire)
puis `make debug`. Le wrapper monte uniquement le fichier ADC en lecture seule ;
ne pas copier de credentials dans le repo ou l'image. `ADC_FILE` permet de
choisir un fichier existant. Les commandes hors ligne n'en ont pas besoin.

Avant une reconstruction, inspecter la sélection, puis exécuter et tester le
périmètre concerné :

```sh
make ls SELECT=tag:activities
make compile SELECT=hub_activities_svc__master_activities
make run SELECT=+hub_activities_svc__master_activities
make test SELECT=tag:activities
make build SELECT=tag:activities
```

- `make docker-build` construit l'image ; **`make build` exécute `dbt build`**.
- `SELECT` accepte un nom de modèle, un chemin ou un tag dbt. `+modele` inclut
  les parents et `modele+` les descendants. Sans sélection, `make run` exécute
  la sélection par défaut du projet : cibler les changements autant que possible.
- `ARGS` transmet les options supplémentaires, par exemple
  `make run SELECT=mon_modele ARGS='--full-refresh'`. Utiliser un full refresh
  ciblé seulement lorsque nécessaire.
- `make dbt ARGS='ls --select tag:health'` donne accès aux autres commandes de
  la CLI dans l'image ; `make freshness SELECT=source:garmin` contrôle les sources.
- `IMAGE=...` choisit une autre image ou un digest publié ; `make docker-pull`
  la récupère. `PLATFORM=linux/amd64` permet d'utiliser cette architecture si
  nécessaire, notamment pour une image publiée sur un Mac Apple Silicon.
- Le code, `dbt_packages/`, `target/` et `logs/` utilisent le workspace local.
  Une modification SQL ne nécessite pas de rebuild de l'image ; un changement
  de dépendances Docker, oui. Relancer `make deps` après un changement de packages dbt.

Si Docker est indisponible, signaler la limitation. Les contrôles hors ligne
ci-dessous restent possibles, mais ne valident pas une exécution dans l'image
ou sur BigQuery.

## Contrôles et documentation

```sh
make setup       # Outils de contribution verrouillés et hooks
make check       # Lint SQL/YAML et parsing via uv, sans écrire de tables
make hooks       # Tous les hooks sur les fichiers suivis
make fix-sql     # Formatage SQL explicite
make docs        # Catalogue dev via Docker ; accès BigQuery en lecture requis
make docs-serve  # Documentation sur localhost:8080 ; PORT=8081 si nécessaire
```

`make parse` utilise Docker ; `make parse-local` utilise uv et sert au parsing
de `make check` et de la CI. SQLFluff utilise Jinja et la version de `uv.lock`
via `scripts/sqlfluff.sh`, en local, dans les hooks et en CI. Ne pas réintroduire
une version ou un templater distincts dans les hooks. Le lint ne reformate pas
automatiquement les fichiers et ne compile pas les requêtes dans BigQuery.

La CI vérifie le lint, le parsing et, selon les fichiers modifiés, l'image avec
les vraies commandes Make. Une CI verte ne remplace pas les tests de données
du périmètre modifié en dev. Documenter modèles, grains, colonnes et unités dans
les `schema.yml` proches des modèles.

## Environnements et promotion

| Cible | Projet GCP | Branche déployée |
|---|---|---|
| `dev` | `ela-dp-dev` | `develop` |
| `prd` | `ela-dp-prd` | `main` |

BigQuery est en région `EU`. Les datasets hub/product ont les mêmes noms dans
les deux projets. Changer seulement `target.schema` n'isole pas les modèles
qui déclarent un schéma personnalisé.

- **Ne jamais committer ou pousser directement sur `main`.** Les changements
  passent par une branche de travail et une PR vers `develop`, puis une PR
  `develop` → `main` pour la production.
- Ne pas lancer de mutation de production sans autorisation explicite. Le fait
  que `TARGET=prd` soit disponible ne constitue pas une autorisation.
- Le workflow CD publie l'image et épingle les jobs Cloud Run à son digest.
  Il ne lance ni reconstruction ni tests. Pour une promotion autorisée, vérifier
  aussi l'exécution du job concerné et les tests de données dans l'environnement.
- **Ne jamais utiliser `polar-scene-*`** : ce projet par défaut n'appartient pas
  à ELA. Cibler explicitement `ela-dp-dev` ou `ela-dp-prd`.

## Règles de modélisation

- Les produits consomment le hub, ou le lake en dernier recours, **jamais un autre
  produit**. Faire remonter la logique partagée dans le hub ; ne pas reproduire
  les exceptions héritées.
- Utiliser `source()` et `ref()`, et préserver les contrats publics des consommateurs.
- Appliquer les tags `spotify` ou `garmin` aux modèles qui consomment ces sources.
  Les domaines Garmin héritent de `health` ou `activities` ; les produits disposent
  aussi de `webapp` et `data4agent`.
- Préserver les corrections historiques et les détails tardifs. Un watermark
  basé uniquement sur le résumé d'activité ne suit pas les changements des courbes
  ou du contexte santé. Voir [LATE_UPDATES.md](LATE_UPDATES.md).
