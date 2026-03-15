---
name: dbt-model
description: Guide for creating, evolving, or debugging dbt models across all medallion layers (lake, hub, product) on BigQuery. Use this skill whenever the user wants to add a new dbt model, modify an existing one, add a new source, create staging/service layers, or work on any SQL transformation in this dbt project. Also trigger when the user mentions table names, BigQuery datasets, or asks about data transformations.
---

# Skill: dbt-model

## Overview

This skill handles the full lifecycle of dbt model development on BigQuery: exploring source data, writing SQL models, documenting them in `schema.yml`, and adding tests — all following the medallion architecture (Lake → Hub → Product).

## Step 1: Understand the request

Before writing any SQL, clarify:
- Which **layer** the model belongs to (lake, hub, or product)
- Which **source** is involved (e.g., spotify)
- What **business logic** or transformation is needed

## Step 2: Explore the data on BigQuery

Use `bq` CLI to understand the source data before writing any model. This avoids guessing column names, types, or data formats.

### Discover available tables in a dataset

```bash
bq ls dp_lake_spotify_dev
```

### Inspect table schema

```bash
bq show --format=prettyjson dp_lake_spotify_dev.table_name | jq '.schema.fields[] | {name, type, mode}'
```

### Preview sample data

```bash
bq query --use_legacy_sql=false --max_rows=10 'SELECT * FROM `dp_lake_spotify_dev.table_name` LIMIT 10'
```

### Check row counts and freshness

```bash
bq query --use_legacy_sql=false 'SELECT COUNT(*) as row_count, MAX(_loaded_at) as latest_load FROM `dp_lake_spotify_dev.table_name`'
```

### Explore specific columns or patterns

```bash
bq query --use_legacy_sql=false 'SELECT DISTINCT column_name FROM `dp_lake_spotify_dev.table_name` LIMIT 20'
```

Always inspect the data first — never assume column names or types.

## Step 3: Write the dbt model

### Directory structure

Organize models by layer, then by source (for lake), then by sub-layer:

```
models/
├── lake/
│   └── spotify/
│       ├── normalized/          # Sources only (no SQL — fed by ingestion)
│       ├── staging/             # Views that deduplicate normalized
│       │   ├── stg_spotify__artists.sql
│       │   └── schema.yml
│       └── service/             # Incremental merge from staging
│           ├── svc_spotify__artists.sql
│           └── schema.yml
├── hub/
│   ├── staging/                 # Views that consolidate/clean from lake service tables
│   │   ├── stg_hub__artists.sql
│   │   └── schema.yml
│   └── service/                 # Incremental merge from hub staging
│       ├── svc_hub__artists.sql
│       └── schema.yml
└── product/
    ├── prd__artist_metrics.sql
    └── schema.yml
```

### Naming conventions

| Layer | Prefix | Example |
|-------|--------|---------|
| Staging (lake) | `stg_<source>__` | `stg_spotify__artists` |
| Service (lake) | `svc_<source>__` | `svc_spotify__artists` |
| Staging (hub) | `stg_hub__` | `stg_hub__artists` |
| Service (hub) | `svc_hub__` | `svc_hub__artists` |
| Product | `prd__` | `prd__artist_metrics` |

Double underscore `__` separates the source/layer prefix from the entity name.

### Source declaration

Sources are declared in `models/lake/<source>/normalized/schema.yml` since normalized tables are external (fed by ingestion, not managed by dbt):

```yaml
version: 2

sources:
  - name: spotify
    database: "{{ target.project }}"
    schema: "dp_lake_spotify_{{ target.name }}"
    description: Raw Spotify data loaded by the ingestion pipeline.
    tables:
      - name: artists
        description: Raw artist data from the Spotify API.
        columns:
          - name: artist_id
            description: Spotify unique artist identifier.
            tests:
              - not_null
```

### Staging model pattern (view, deduplication)

Staging models are **views** that deduplicate data from the normalized layer:

```sql
-- stg_spotify__artists.sql
{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

with source as (
    select * from {{ source('spotify', 'artists') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by artist_id
            order by _loaded_at desc
        ) as _row_number
    from source
)

select * except(_row_number)
from deduplicated
where _row_number = 1
```

### Service model pattern (incremental merge)

Service models are **incremental tables** that merge new data from staging:

```sql
-- svc_spotify__artists.sql
{{
    config(
        materialized='incremental',
        unique_key='artist_id',
        merge_update_columns=['name', 'genres', 'popularity', 'followers', '_loaded_at'],
        tags=['spotify']
    )
}}

select *
from {{ ref('stg_spotify__artists') }}

{% if is_incremental() %}
where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
```

### Hub staging model pattern (view, consolidation)

Hub staging models are **views** that clean, normalize, and consolidate data from one or more lake service tables:

```sql
-- stg_hub__artists.sql
{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

select
    artist_id,
    lower(trim(name)) as artist_name,
    genres,
    popularity,
    followers,
    _loaded_at
from {{ ref('svc_spotify__artists') }}
```

### Hub service model pattern (incremental merge)

Hub service models are **incremental tables** that merge new data from hub staging:

```sql
-- svc_hub__artists.sql
{{
    config(
        materialized='incremental',
        unique_key='artist_id',
        merge_update_columns=['artist_name', 'genres', 'popularity', 'followers', '_loaded_at'],
        tags=['spotify']
    )
}}

select *
from {{ ref('stg_hub__artists') }}

{% if is_incremental() %}
where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
```

### Product model pattern (business-ready)

Product models shape data for consumption (dashboards, APIs):

```sql
-- prd__artist_metrics.sql
{{
    config(
        materialized='table',
        tags=['spotify']
    )
}}

select
    artist_id,
    artist_name,
    popularity,
    followers,
    array_length(genres) as genre_count
from {{ ref('hub__artists') }}
```

## Step 4: Document in schema.yml

Every model directory must have a `schema.yml`. Document **every model and every column** with clear English descriptions.

```yaml
version: 2

models:
  - name: stg_spotify__artists
    description: Deduplicated view of raw Spotify artist data from the normalized layer.
    columns:
      - name: artist_id
        description: Spotify unique artist identifier.
        tests:
          - unique
          - not_null
      - name: name
        description: Artist display name.
        tests:
          - not_null
      - name: _loaded_at
        description: Timestamp when the record was loaded into the normalized table.
```

### Tests to apply systematically

| Test | When to use |
|------|-------------|
| `unique` | On primary keys and natural keys |
| `not_null` | On primary keys, foreign keys, and required fields |
| `accepted_values` | On status fields, enums, or categorical columns |
| `relationships` | On foreign keys referencing another model |

## Step 5: Configure dbt_project.yml

When adding a new layer or source, update `dbt_project.yml` to set default materializations and schemas:

```yaml
models:
  ela_dp:
    lake:
      +schema: "dp_lake_{{ var('source_name') }}_{{ target.name }}"
      staging:
        +materialized: view
      service:
        +materialized: incremental
    hub:
      +schema: "dp_hub_{{ target.name }}"
      staging:
        +materialized: view
      service:
        +materialized: incremental
    product:
      +schema: "dp_product_{{ target.name }}"
      +materialized: table
```

## Step 6: Validate

After creating or modifying models, always run:

```bash
# Check SQL compiles correctly
dbt compile --select model_name

# Run the model
dbt run --select model_name

# Run tests
dbt test --select model_name
```

## Tags

Apply the source tag (e.g., `spotify`) to **every** model that touches that source's data — this enables targeted runs with `dbt run --select tag:spotify`. Set it in the `config()` block of each model.

## Checklist before finishing

- [ ] Data explored on BigQuery (schema + sample)
- [ ] SQL model created in the correct directory
- [ ] `schema.yml` with descriptions for all models and columns
- [ ] Tests added (unique, not_null at minimum on keys)
- [ ] Tags applied in `config()` block
- [ ] `dbt compile` succeeds
