{{ config(
    materialized='table',
    schema=env_var('SINGLESTORE_SCHEMA')
) }}

WITH source AS (
  SELECT
    DATE_FORMAT(FROM_UNIXTIME(eventTime/1000), '%Y-%m-%d 00:00:00')
      AS day_bucket,
    assetNumber                                AS asset_number,
    ess,
    gameId                                     AS game_id,
    devInfoGameTheme                           AS game_name,
    CASE
      WHEN playerCardNumber IS NULL
        OR playerCardNumber = '' THEN 'uncarded'
      ELSE 'carded'
    END                                        AS player_type,

    -- explode metrics for aggregation
    updateCashableWager 
      + updateRestrictedWager 
      + updateOORVCashableWager 
      + updateOORVRestrictedWager AS coin_in,
    updateCashableWager 
      + updateOORVCashableWager       AS cashable_coin_in,
    updateWon 
      + updateOORVWon                 AS coin_out,
    updateJackpot 
      + updateOORVJackpot             AS jackpot,
    maxCashableRisked                 AS budget,

    sessionUuid,
    assetNumber    AS raw_asset,
    devInfoGameTheme AS raw_theme,
    playerCardNumber,
    duration,
    

  FROM {{ env_var('SINGLESTORE_DB') }}.session_data

  WHERE type = 'STUpdate'
    AND assetNumber IS NOT NULL

  {% if is_incremental() %}
    AND DATE(FROM_UNIXTIME(eventTime/1000)) 
        >= (SELECT MAX(day_bucket) FROM {{ this }})
  {% endif %}
),

-- 2) Compute carded vs uncarded aggregates
CardedUncarded AS (
  SELECT
    day_bucket,
    asset_number,
    ess,
    game_id,
    game_name,
    player_type,

    SUM(coin_in)         AS total_coin_in,
    SUM(cashable_coin_in)AS total_cashable_coin_in,
    SUM(coin_out)        AS total_coin_out,
    SUM(jackpot)         AS total_jackpot,

    -- true net win
    SUM(coin_in)
      - SUM(coin_out)
      - SUM(jackpot)     AS total_net_win,

    MAX(budget)          AS max_player_budget,

    AVG(coin_in)         AS total_avg_bet_size,
    COUNT(*)             AS total_spins,
    COUNT(DISTINCT sessionUuid)       AS total_session_counts,
    COUNT(DISTINCT raw_asset)         AS total_distinct_asset_counts,
    COUNT(DISTINCT raw_theme)         AS total_distinct_game_themes,
    COUNT(DISTINCT playerCardNumber)  AS total_distinct_player_card_numbers,
    SUM(duration)               AS total_duration,
    AVG(duration) as avg_duration

  FROM source
  GROUP BY
    day_bucket,
    asset_number,
    ess,
    game_id,
    game_name,
    player_type
),

-- 3) Compute true distincts for the “All” bucket
DistinctTotals AS (
  SELECT
    day_bucket,
    asset_number,
    ess,
    game_id,
    game_name,
    COUNT(DISTINCT sessionUuid)       AS total_session_counts,
    COUNT(DISTINCT raw_asset)         AS total_distinct_asset_counts,
    COUNT(DISTINCT raw_theme)         AS total_distinct_game_themes,
    COUNT(DISTINCT playerCardNumber)  AS total_distinct_player_card_numbers,
    SUM(duration)               AS total_duration,
    AVG(duration) as avg_duration
  FROM source
  GROUP BY
    day_bucket,
    asset_number,
    ess,
    game_id,
    game_name
)

-- 4) Emit carded + uncarded
SELECT * FROM CardedUncarded

UNION ALL

-- 5) Emit the “All” row by re-aggregating CU metrics + plugging in true distincts
SELECT
  cu.day_bucket,
  cu.asset_number,
  cu.ess,
  cu.game_id,
  cu.game_name,
  'All'                                             AS player_type,

  -- numeric metrics summed
  SUM(cu.total_coin_in)          AS total_coin_in,
  SUM(cu.total_cashable_coin_in) AS total_cashable_coin_in,
  SUM(cu.total_coin_out)         AS total_coin_out,
  SUM(cu.total_jackpot)          AS total_jackpot,
  SUM(cu.total_net_win)          AS total_net_win,

  -- budget: highest seen
  MAX(cu.max_player_budget)      AS max_player_budget,

  -- weighted avg bet size
  SUM(cu.total_coin_in)
    / NULLIF(SUM(cu.total_spins), 0)                  AS total_avg_bet_size,

  -- spins
  SUM(cu.total_spins)            AS total_spins,

  SUM(cu.total_duration) as total_duration,
  SUM(cu.total_duration)/NULLIF(SUM(cu.total_spins), 0) as avg_duration,

  -- true distincts from DistinctTotals
  dt.total_session_counts,
  dt.total_distinct_asset_counts,
  dt.total_distinct_game_themes,
  dt.total_distinct_player_card_numbers

FROM CardedUncarded cu
JOIN DistinctTotals dt
  USING (day_bucket, asset_number, ess, game_id, game_name)

GROUP BY
  cu.day_bucket,
  cu.asset_number,
  cu.ess,
  cu.game_id,
  cu.game_name
;

