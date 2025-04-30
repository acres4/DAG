{{ config(
    materialized='table',
    schema=env_var('SINGLESTORE_SCHEMA')
) }}

WITH source AS (
  SELECT
    DATE(FROM_UNIXTIME(eventTime/1000)) AS day_bucket,
    gameId                             AS game_id,
    ess,
    SUM(updateCashableWager)           AS total_coin_in,
    SUM(updateWon)                     AS total_coin_out,
    AVG(updateCashableWager)           AS avg_bet_size,
    AVG(
      CASE WHEN duration > 0
           THEN updateCashableWager / duration
           ELSE NULL
      END
    )                                  AS avg_bet_velocity,
    COUNT(*)                           AS spin_count,
    COUNT(DISTINCT sessionUuid)        AS session_count,
    COUNT(DISTINCT CASE
      WHEN playerCardNumber IS NOT NULL
           AND playerCardNumber <> ''
      THEN sessionUuid
    END)                                AS carded_sessions,
    COUNT(DISTINCT CASE
      WHEN playerCardNumber IS NULL
           OR playerCardNumber = ''
      THEN sessionUuid
    END)                                AS uncarded_sessions,
    COUNT(DISTINCT CASE
      WHEN playerCardNumber IS NOT NULL
           AND playerCardNumber <> ''
      THEN playerCardNumber
    END)                                AS unique_player_card_numbers
  FROM {{ env_var('SINGLESTORE_DB') }}.session_data
  WHERE type = 'STUpdate'
  GROUP BY day_bucket, game_id, ess
)

SELECT
  day_bucket,
  game_id,
  ess,
  total_coin_in,
  total_coin_out,
  avg_bet_size,
  avg_bet_velocity,
  spin_count,
  session_count,
  carded_sessions,
  uncarded_sessions,
  unique_player_card_numbers
FROM source;
