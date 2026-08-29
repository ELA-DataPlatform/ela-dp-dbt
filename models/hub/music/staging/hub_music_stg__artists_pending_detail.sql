{{
    config(
        materialized='view',
        tags=['spotify']
    )
}}

-- Worklist des artistes dont on n'a PAS encore récupéré le détail Spotify.
-- Pendant de `hub_music_stg__albums_pending_detail`, consommée par `ela-dp-spotify-artist-detail`.
-- Même contrat : un artiste sort du pending dès qu'il est dans `dlk_spotify_svc__artist_detail`
-- (ou via row sentinelle écrite par l'ingestion pour les échecs permanents).
--
-- Récence approximée via les artistes de piste (bridge_track_artist). Les artistes qui
-- n'apparaissent qu'en tant qu'artiste d'album (jamais de piste jouée) ont last_played_at
-- NULL et passent en fin de file. `LIMIT` piloté par `artist_detail_batch_size` (défaut 50).

WITH universe AS (
    SELECT artist_id
    FROM {{ ref('hub_music_stg__all_artist_ids') }}
    WHERE artist_id IS NOT NULL
),

already_detailed AS (
    SELECT artist_id
    FROM {{ ref('dlk_spotify_svc__artist_detail') }}
),

last_played AS (
    SELECT
        b.artist_id,
        MAX(p.played_at) AS last_played_at
    FROM {{ ref('hub_music_stg__fact_played') }} AS p
    INNER JOIN {{ ref('hub_music_stg__bridge_track_artist') }} AS b ON p.track_id = b.track_id
    WHERE b.artist_id IS NOT NULL
    GROUP BY b.artist_id
),

pending AS (
    SELECT
        u.artist_id,
        lp.last_played_at
    FROM universe AS u
    LEFT JOIN already_detailed AS d ON u.artist_id = d.artist_id
    LEFT JOIN last_played AS lp ON u.artist_id = lp.artist_id
    WHERE d.artist_id IS NULL
)

SELECT
    artist_id,
    last_played_at
FROM pending
ORDER BY last_played_at DESC NULLS LAST
LIMIT {{ var('artist_detail_batch_size', 50) }}
