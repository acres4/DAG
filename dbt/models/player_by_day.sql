{{ config(
    materialized='incremental',
    unique_key=['day_bucket', 'playerCardNumber']
) }}

WITH source AS (

  SELECT
    -- daily bucket
    DATE(FROM_UNIXTIME(eventTime/1000))    AS day_bucket,

    playerCardNumber,

    -- metrics
    SUM(updateCashableWager)               AS total_coin_in,
    SUM(updateWon)                         AS total_coin_out,
    AVG(updateCashableWager)               AS avg_bet_size,
    AVG(
      CASE WHEN duration > 0 
           THEN updateCashableWager / duration 
           ELSE NULL 
      END
    )                                      AS avg_bet_velocity,
    COUNT(*)                               AS spin_count,
    COUNT(DISTINCT sessionUuid)            AS session_count,

    -- optional additional metrics
    SUM(duration)                          AS total_duration,
    AVG(duration)                          AS avg_duration

  FROM {{ env_var('SINGLESTORE_DB') }}.session_data

  WHERE type = 'STUpdate'
    AND playerCardNumber IS NOT NULL
    AND playerCardNumber <> ''

  {% if is_incremental() %}
    AND DATE(FROM_UNIXTIME(eventTime/1000)) 
        >= (SELECT MAX(day_bucket) FROM {{ this }})
  {% endif %}

  GROUP BY day_bucket, playerCardNumber

)

SELECT * FROM source
