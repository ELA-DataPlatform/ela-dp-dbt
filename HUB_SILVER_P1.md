# P1 — Fiabilisation du hub

Implémentation du 18/10/2026 sur `feature/hub-silver-p1`.
Périmètre : hub Music et master Garmin. Les tests sont reportés à la demande
de l'utilisateur : aucun ajout, retrait ou lancement de tests ; CI, Dockerfile
et orchestration restent inchangés.

## Choix réalisés

- Trois vues internes `track_observations`, `album_observations` et
  `artist_observations` rapprochent les sources Spotify modernes et legacy.
  Elles conservent plusieurs observations par clé et alimentent références,
  bridges et univers d'enrichissement.
- Les titres sauvegardés sont inclus ; tous les albums observés dans les titres,
  y compris les seules clés parentes de `album_tracks`, sont référencés.
  Les artistes de toutes les listes de titres et d'albums sont référencés,
  même s'ils n'ont jamais été écoutés ou enrichis.
- Les référentiels privilégient une observation nommée, puis la priorité de source,
  sa date d'ingestion décroissante, son nom et son contenu pour départager.
  Certains enrichissements nuls sont complétés depuis les autres observations.
  Les attributs structurels restent ceux du même enregistrement canonique.
- `metadata_source` indique la source du socle canonique, sans prétendre donner
  la provenance de chaque attribut complété. `_ingested_at` des références est
  le maximum des ingestions des observations de l'entité.
- Albums et artistes exposent `enrichment_status` : `complete` si un détail
  possède un nom exploitable, `unavailable` si des détails existent sans nom
  exploitable, `pending` en l'absence de détail. Ce statut ne garantit pas que
  chaque attribut facultatif soit renseigné et n'infère pas un code d'erreur API.
- Chaque bridge sélectionne une liste complète d'artistes par entité avant de
  l'aplatir. Les artistes répétés sont regroupés et les positions réindexées.
  Les listes vides peuvent utiliser une observation de repli non vide.
- Les trois référentiels et les deux bridges sont des tables reconstruites à
  chaque exécution. Les associations obsolètes disparaissent et les corrections
  historiques ne dépendent plus d'un watermark global.
- Le fait d'écoute conserve son merge et ses partitions, en rejouant toute la
  source dédupliquée. Les timestamps égaux, les corrections anciennes et une cible
  vide ne sont plus exclus par un filtre incrémental. La sélection entre sources
  est déterministe. Le coût sera à réévaluer si le volume augmente fortement.
- Le master Garmin reste au grain d'une course et conserve partitions et cluster.
  Il est intégralement reconstruit pour reprendre routes et changements dans
  toutes ses dépendances, y compris les intervalles de validité poids/VO2max.
  La colonne `status_source.training_status` est explicitement qualifiée :
  `athletic_context.training_status_code` est désormais `INT64`.

Les listes d'enrichissement couvrent maintenant le même univers que les
référentiels ; leur limite de lot reste inchangée. Leur backlog peut augmenter
à mesure que les artistes du catalogue non joué deviennent éligibles.

## Validation réalisée en dev

- Lint SQLFluff des deux domaines et compilation dbt réussis.
- Chargement des quatre schémas YAML réussi ; le binaire yamllint n'est pas
  disponible dans l'environnement local.
- `dbt run` ciblé : 21 modèles construits avec succès, aucun test exécuté.
  Un merge, six tables et quatorze vues ; environ 535 Mio traités au total
  lors de cette exécution, dont 165 Mio pour le master Garmin.
- Lecture agrégée après construction : zéro relation titre–artiste ou
  album–artiste orpheline, zéro titre sans album référencé, zéro titre ou album
  avec plusieurs artistes principaux. Il s'agit du résultat en dev, pas d'une
  correction déjà appliquée aux données de production.
- Le consommateur `pct_webapp_activity_focus__activity_detail` a été reconstruit
  avec succès en dev (363 lignes) après l'échec attendu du merge sur l'ancien
  type. Les métadonnées BigQuery confirment `INT64` dans le hub et le produit.
  Cette migration nécessite une reconstruction complète de la table existante.

Commande de construction du hub (exécutée en dev) :

```sh
dbt run --profiles-dir . --target dev \
  --select path:models/hub/music path:models/hub/activities
```

Puis, une fois le hub reconstruit, migration du consommateur :

```sh
dbt run --profiles-dir . --target dev --full-refresh \
  --select pct_webapp_activity_focus__activity_detail
```

## Promotion et limites

La production n'a pas été modifiée. Promouvoir via les PR du dépôt, puis
construire le hub avant les produits. À la première promotion, la reconstruction
complète du consommateur ci-dessus est nécessaire avec la cible `prd` : son
merge habituel échoue tant que la colonne physique possède l'ancien type.
Vérifier aussi les contrats API qui exposent ce champ corrigé.

Cette intervention ne modifie pas les filtres incrémentaux du bronze : seules
les corrections effectivement présentes dans ses services sont disponibles au
hub. Les modèles gold qui filtrent sur le timestamp de l'activité principale
peuvent encore manquer des changements d'enrichissement. Leur stratégie de
rafraîchissement reste un chantier distinct ; le `_ingested_at` du master
continue volontairement de représenter l'ingestion de l'activité principale.
Une reconstruction gold reprend le nouvel état silver, mais ce P1 ne garantit
pas sa propagation incrémentale automatique à tous les produits.

Le chantier de tests et de blocage de publication décrit dans
[la proposition](HUB_SILVER_PROPOSAL.md) reste à traiter dans un second temps.
