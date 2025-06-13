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
    -- explode metrics so we can aggregate in CTEs
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
    devInfoGameThemes,
    playerCardNumber
  FROM {{ env_var('SINGLESTORE_DB') }}.session_data
  WHERE type = 'STUpdate'
    AND floorLocation IS NOT NULL

  {% if is_incremental() %}
    AND FROM_UNIXTIME(eventTime/1000)
        >= (SELECT MAX(hour_bucket) FROM {{ this }})
  {% endif %}
),

-- 2) Build the carded / uncarded aggregates
CardedUncarded AS (
  SELECT
    hour_bucket,
    floor_location,
    player_type,
    SUM(coin_in)         AS total_coin_in,
    SUM(cashable_coin_in)AS total_cashable_coin_in,
    SUM(coin_out)        AS total_coin_out,
    SUM(jackpot)         AS total_jackpot,

    -- real net win
    SUM(coin_in)
      - SUM(coin_out)
      - SUM(jackpot)     AS total_net_win,

    MAX(budget)          AS max_player_budget,

    AVG(coin_in)         AS total_avg_bet_size,
    COUNT(*)             AS total_spins,
    COUNT(DISTINCT sessionUuid)            AS total_session_counts,
    COUNT(DISTINCT assetNumber)            AS total_distinct_asset_counts,
    COUNT(DISTINCT devInfoGameThemes)      AS total_distinct_game_themes,
    COUNT(DISTINCT playerCardNumber)       AS total_distinct_player_card_numbers

  FROM source
  GROUP BY hour_bucket, floor_location, player_type
),

-- 3) Compute the *true* distinct counts for the “All” bucket
DistinctTotals AS (
  SELECT
    hour_bucket,
    floor_location,
    COUNT(DISTINCT sessionUuid)            AS total_session_counts,
    COUNT(DISTINCT assetNumber)            AS total_distinct_asset_counts,
    COUNT(DISTINCT devInfoGameThemes)      AS total_distinct_game_themes,
    COUNT(DISTINCT playerCardNumber)       AS total_distinct_player_card_numbers
  FROM source
  GROUP BY hour_bucket, floor_location
)

-- 4) Emit carded + uncarded
SELECT * FROM CardedUncarded

UNION ALL

-- 5) Emit the “All” row by re-aggregating C.U. metrics and plugging in DistinctTotals
SELECT
  cu.hour_bucket,
  cu.floor_location,
  'All'                                     AS player_type,

  -- sum the numeric metrics
  SUM(cu.total_coin_in)          AS total_coin_in,
  SUM(cu.total_cashable_coin_in) AS total_cashable_coin_in,
  SUM(cu.total_coin_out)         AS total_coin_out,
  SUM(cu.total_jackpot)          AS total_jackpot,
  SUM(cu.total_net_win)          AS total_net_win,

  -- budget: take the max across both
  MAX(cu.max_player_budget)      AS max_player_budget,

  -- weighted avg bet size (total_coin_in / total_spins)
  SUM(cu.total_coin_in) 
    / NULLIF(SUM(cu.total_spins), 0)       AS total_avg_bet_size,

  -- spins
  SUM(cu.total_spins)            AS total_spins,

  -- true distinct counts from DistinctTotals
  dt.total_session_counts,
  dt.total_distinct_asset_counts,
  dt.total_distinct_game_themes,
  dt.total_distinct_player_card_numbers

FROM CardedUncarded cu
JOIN DistinctTotals dt
  USING (hour_bucket, floor_location)

GROUP BY cu.hour_bucket, cu.floor_location
;
