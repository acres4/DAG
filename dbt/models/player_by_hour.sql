{{ config(
    materialized='incremental',
    unique_key=['hour_bucket','playerCardNumber']
) }}

WITH source AS (

  SELECT
    DATE_FORMAT(
      FROM_UNIXTIME(eventTime/1000),
      '%Y-%m-%d %H:00:00'
    )                 AS hour_bucket,
    playerCardNumber,

    SUM(updateCashableWager)    AS total_coin_in,
    SUM(updateWon)              AS total_coin_out,
    AVG(updateCashableWager)    AS avg_bet_size,
    AVG(
      CASE WHEN duration > 0 
           THEN updateCashableWager / duration 
           ELSE NULL END
    )                           AS avg_bet_velocity,
    COUNT(*)                    AS spin_count,
    COUNT(DISTINCT sessionUuid) AS session_count,

    -- new aggregates
    SUM(duration)               AS total_duration,
    AVG(duration)               AS avg_duration,
    MAX(updateCashableWager)    AS max_bet_size,
    MIN(updateCashableWager)    AS min_bet_size,
    MAX(maxCashableRisked)      AS max_cashable_risked

  FROM {{ env_var('SINGLESTORE_DB') }}.session_data

  WHERE type = 'STUpdate'
    -- ❗ only include rows with a non-null, non-empty card number
    AND playerCardNumber IS NOT NULL
    AND playerCardNumber <> ''

  {% if is_incremental() %}
    AND FROM_UNIXTIME(eventTime/1000) 
        >= (SELECT MAX(hour_bucket) FROM {{ this }})
  {% endif %}

  GROUP BY hour_bucket, playerCardNumber

)

SELECT * FROM source