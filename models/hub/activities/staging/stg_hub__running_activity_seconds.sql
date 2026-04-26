{{
    config(
        materialized='view',
        tags=['garmin']
    )
}}

-- Per-~2-second timeseries for all running activities.
-- Source: svc_garmin__activity_details.detailed_data (Garmin activityDetailMetrics JSON)
-- Note: the service layer keeps only the latest ingestion per activity, which may
-- represent one page of the Garmin API response for long activities.

with
running_activity_ids as (
    select
        activityId   as activity_id,
        date(startTimeLocal) as activity_date,
        json_value(activityType, '$.typeKey') as activity_type
    from {{ ref('svc_garmin__activities') }}
    where json_value(activityType, '$.typeKey') in ('running', 'trail_running', 'treadmill_running')
),

activity_details as (
    select
        activityId,
        detailed_data,
        _ingested_at
    from {{ ref('svc_garmin__activity_details') }}
    where detailed_data is not null
),

running_details as (
    select
        r.activity_id,
        r.activity_date,
        r.activity_type,
        d.detailed_data,
        d._ingested_at
    from running_activity_ids r
    inner join activity_details d
        on d.activityId = r.activity_id
),

-- Unnest the activityDetailMetrics JSON array into one row per measurement
unnested as (
    select
        rd.activity_id,
        rd.activity_date,
        rd.activity_type,
        rd._ingested_at,
        -- Timestamp in ms (stored as float in Garmin JSON, e.g. 1773768684000.0)
        timestamp_millis(
            cast(floor(cast(json_value(m, '$.directTimestamp') as float64)) as int64)
        )                                                   as timestamp_gmt,
        -- Elapsed seconds since activity start (cumulative)
        cast(json_value(m, '$.sumElapsedDuration') as float64) as elapsed_s,
        cast(json_value(m, '$.sumDuration') as float64)        as duration_s,
        cast(json_value(m, '$.sumMovingDuration') as float64)  as moving_duration_s,
        -- Distance (cumulative, meters)
        cast(json_value(m, '$.sumDistance') as float64)        as cum_distance_m,
        -- Speed (m/s) → convert to pace (min/km) inline for convenience
        cast(json_value(m, '$.directSpeed') as float64)        as speed_m_per_s,
        -- Cardiac and respiratory
        cast(json_value(m, '$.directHeartRate') as float64)    as heart_rate_bpm,
        -- Running dynamics
        cast(json_value(m, '$.directDoubleCadence') as float64) as cadence_spm,
        cast(json_value(m, '$.directFractionalCadence') as float64) as fractional_cadence,
        -- Wellness
        cast(json_value(m, '$.directBodyBattery') as float64)  as body_battery,
        -- GPS position
        cast(json_value(m, '$.directLatitude') as float64)     as latitude,
        cast(json_value(m, '$.directLongitude') as float64)    as longitude
    from running_details rd,
    unnest(json_query_array(rd.detailed_data, '$.activityDetailMetrics')) as m
)

select *
from unnested
where timestamp_gmt is not null
