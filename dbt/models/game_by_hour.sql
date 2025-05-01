{{ config(
    materialized='incremental',
    unique_key=['hour_bucket','game_id','ess']
) }}

WITH source AS (

  SELECT
    -- hourly bucket
    DATE_FORMAT(
      FROM_UNIXTIME(eventTime/1000),
      '%Y-%m-%d %H:00:00'
    )                                  AS hour_bucket,

    -- game identifier
    gameId                              AS game_id,

    -- equipment/session serial
    ess,

    -- basic wager/win metrics
    SUM(updateCashableWager)            AS total_coin_in,
    SUM(updateWon)                      AS total_coin_out,
    AVG(updateCashableWager)            AS avg_bet_size,
    AVG(
      CASE WHEN duration > 0 
           THEN updateCashableWager / duration 
           ELSE NULL 
      END
    )                                   AS avg_bet_velocity,

    -- activity counts
    COUNT(*)                            AS spin_count,
    COUNT(DISTINCT sessionUuid)         AS session_count,

    -- carded vs. uncarded sessions
    COUNT(
      DISTINCT CASE 
        WHEN playerCardNumber IS NOT NULL 
             AND playerCardNumber <> '' 
        THEN sessionUuid 
      END
    )                                   AS carded_sessions,

    COUNT(
      DISTINCT CASE 
        WHEN playerCardNumber IS NULL 
             OR playerCardNumber = '' 
        THEN sessionUuid 
      END
    )                                   AS uncarded_sessions,

    -- unique players per game/hour/ess
    COUNT(
      DISTINCT CASE 
        WHEN playerCardNumber IS NOT NULL 
             AND playerCardNumber <> '' 
        THEN playerCardNumber 
      END
    )                                   AS unique_player_card_numbers

  FROM {{ env_var('SINGLESTORE_DB') }}.session_data

  WHERE type = 'STUpdate'

  {% if is_incremental() %}
    -- only process new hours since last run
    AND FROM_UNIXTIME(eventTime/1000) 
        >= (SELECT MAX(hour_bucket) FROM {{ this }})
  {% endif %}

  GROUP BY hour_bucket, game_id, ess

)

SELECT * 
FROM source;
