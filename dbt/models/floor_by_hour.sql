{{ config(
    materialized='incremental',
    unique_key=['hour_bucket','floor_location','player_type']
) }}

# NOTE: 
# TODO: for the floor location, check with patrick to see if we want first 2 or first 4 digitzs

-- 1) Grab raw events once, tag player_type, and filter incrementally
WITH source AS (
  SELECT
    DATE_FORMAT(FROM_UNIXTIME(eventTime/1000), '%Y-%m-%d %H:00:00') AS hour_bucket,
    floorLocation                             AS floor_location,
    CASE 
      WHEN playerCardNumber IS NULL
        OR playerCardNumber = '' THEN 'uncarded'
      ELSE 'carded'
    END                                       AS player_type,
    updateCashableWager
      + updateRestrictedWager
      + updateOORVCashableWager
      + updateOORVRestrictedWager            AS coin_in,
    updateCashableWager
      + updateOORVCashableWager              AS cashable_coin_in,
    updateWon
      + updateOORVWon                        AS coin_out,
    updateJackpot
      + updateOORVJackpot                    AS jackpot,
    maxCashableRisked                        AS budget,
    sessionUuid,
    assetNumber,
    devInfoGameTheme,
    playerCardNumber,
    duration
  FROM mc_sessions
  WHERE type = 'STUpdate'
    AND floorLocation IS NOT NULL
    AND eventTime >= 1749522949000
),

CardedUncarded AS (
  SELECT
    hour_bucket,
    floor_location,
    player_type,

    SUM(coin_in)         AS total_coin_in,
    SUM(cashable_coin_in)AS total_cashable_coin_in,
    SUM(coin_out)        AS total_coin_out,
    SUM(jackpot)         AS total_jackpot,
    SUM(coin_in) - SUM(coin_out) - SUM(jackpot)  AS total_net_win,
    MAX(budget)          AS max_player_budget,
    AVG(coin_in)         AS total_avg_bet_size,
    COUNT(*)             AS total_spins,
    COUNT(DISTINCT sessionUuid)       AS total_session_counts,
    COUNT(DISTINCT assetNumber)       AS total_distinct_asset_counts,
    COUNT(DISTINCT devInfoGameTheme)  AS total_distinct_game_themes,
    COUNT(DISTINCT playerCardNumber)  AS total_distinct_player_card_numbers,
    SUM(duration)        AS total_duration,
    AVG(duration)        AS avg_duration
  FROM source
  GROUP BY hour_bucket, floor_location, player_type
),

DistinctTotals AS (
  SELECT
    hour_bucket,
    floor_location,
    COUNT(DISTINCT sessionUuid)       AS total_session_counts,
    COUNT(DISTINCT assetNumber)       AS total_distinct_asset_counts,
    COUNT(DISTINCT devInfoGameTheme)  AS total_distinct_game_themes,
    COUNT(DISTINCT playerCardNumber)  AS total_distinct_player_card_numbers,
    SUM(duration)                     AS total_duration
  FROM source
  GROUP BY hour_bucket, floor_location
), Final as (

-- 1) Carded + Uncarded
SELECT * FROM CardedUncarded

UNION ALL

-- 2) The “All” bucket
SELECT
  dt.hour_bucket,
  dt.floor_location,
  'All'                                   AS player_type,

  -- Numeric metrics summed
  SUM(cu.total_coin_in)          AS total_coin_in,
  SUM(cu.total_cashable_coin_in) AS total_cashable_coin_in,
  SUM(cu.total_coin_out)         AS total_coin_out,
  SUM(cu.total_jackpot)          AS total_jackpot,
  SUM(cu.total_net_win)          AS total_net_win,

  -- Max budget across both types
  MAX(cu.max_player_budget)      AS max_player_budget,

  -- Weighted average bet size
  SUM(cu.total_coin_in)
    / NULLIF(SUM(cu.total_spins),0)     AS total_avg_bet_size,

  SUM(cu.total_spins)            AS total_spins,

  -- True distinct counts from DistinctTotals
  dt.total_session_counts,
  dt.total_distinct_asset_counts,
  dt.total_distinct_game_themes,
  dt.total_distinct_player_card_numbers,

  -- Duration metrics
  SUM(cu.total_duration)         AS total_duration,
  SUM(cu.total_duration)
    / NULLIF(SUM(cu.total_spins),0)     AS avg_duration

FROM CardedUncarded cu
JOIN DistinctTotals   dt
  USING (hour_bucket, floor_location)

GROUP BY
  dt.hour_bucket,
  dt.floor_location,
  dt.total_session_counts,
  dt.total_distinct_asset_counts,
  dt.total_distinct_game_themes,
  dt.total_distinct_player_card_numbers
) select * from Final order by hour_bucket
;

