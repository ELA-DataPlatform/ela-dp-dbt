{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

-- Worklist des albums dont on n'a PAS encore récupéré le détail Spotify.
-- Consommée par le job d'ingestion `ela-dp-spotify-album-detail` en mode incrémental :
-- il ne fetch que ces album_ids (par petits lots) au lieu de refetch tout le catalogue,
-- ce qui borne la mémoire du job (fini l'OOM à 512Mi sur ~2000 albums).
--
-- Un album quitte cette liste dès qu'il apparaît dans `dlk_spotify_svc__album_detail`.
-- Les échecs permanents (id supprimé, region-lock) sont gérés côté ingestion via une
-- row sentinelle écrite dans le détail (images vides) → ils sortent aussi du pending.
--
-- Tri par dernière écoute décroissante : les albums récemment joués (donc visibles dans
-- les classements /music) sont enrichis en premier. `LIMIT` piloté par la var
-- `album_detail_batch_size` (défaut 50) pour rester sous la limite mémoire / rate-limit.

WITH universe AS (
    SELECT album_id
    FROM {{ ref('hub_music_stg__all_album_ids') }}
    WHERE album_id IS NOT NULL
),

already_detailed AS (
    SELECT album_id
    FROM {{ ref('dlk_spotify_svc__album_detail') }}
),

last_played AS (
    SELECT
        album_id,
        MAX(played_at) AS last_played_at
    FROM {{ ref('hub_music_stg__fact_played') }}
    WHERE album_id IS NOT NULL
    GROUP BY album_id
),

pending AS (
    SELECT
        u.album_id,
        lp.last_played_at
    FROM universe AS u
    LEFT JOIN already_detailed AS d ON u.album_id = d.album_id
    LEFT JOIN last_played AS lp ON u.album_id = lp.album_id
    WHERE d.album_id IS NULL
)

SELECT
    album_id,
    last_played_at
FROM pending
ORDER BY last_played_at DESC NULLS LAST
LIMIT {{ var('album_detail_batch_size', 50) }}
