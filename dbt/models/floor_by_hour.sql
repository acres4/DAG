{{ config(
    materialized='incremental',
    unique_key=['hour_bucket', 'floor_location']
) }}

WITH source AS (

  SELECT
    -- hourly bucket
    DATE_FORMAT(
      FROM_UNIXTIME(eventTime/1000),
      '%Y-%m-%d %H:00:00'
    )                                 AS hour_bucket,

    -- group by floor location
    floorLocation                     AS floor_location,

    -- metrics
    AVG(updateCashableWager)          AS avg_bet_size,
    COUNT(*)                          AS total_wagers,
    SUM(updateCashableWager)          AS total_coin_in,
    AVG(
      CASE 
        WHEN duration > 0 
        THEN updateCashableWager / duration 
        ELSE NULL 
      END
    )                                 AS avg_bet_velocity

  FROM {{ env_var('SINGLESTORE_DB') }}.session_data

  WHERE type = 'STUpdate'
    -- skip null floor locations, if any
    AND floorLocation IS NOT NULL

  {% if is_incremental() %}
    -- only process new hours
    AND FROM_UNIXTIME(eventTime/1000) 
        >= (SELECT MAX(hour_bucket) FROM {{ this }})
  {% endif %}

  GROUP BY hour_bucket, floor_location

)

SELECT * FROM source