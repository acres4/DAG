{{ config(
    materialized='incremental',
    unique_key=['hour_bucket', 'player_card_number']
) }}

-- Note: 
-- It was noted in the original spec to add Max Player Budget by Game
-- Also avg bet velocity as time between plays - Skipped this version
--


----------------------------------------------------------------------------------
-- 1) Pulling out data need
-----------------------------------------------------------------------------------

WITH source AS (
  SELECT
    DATE_FORMAT(FROM_UNIXTIME(eventTime/1000), '%Y-%m-%d %H:00:00')
      AS hour_bucket,
    playerCardNumber AS player_card_number,

    -- exploded metrics
    updateCashableWager
      + updateRestrictedWager
      + updateOORVCashableWager
      + updateOORVRestrictedWager    AS coin_in,
    updateCashableWager
      + updateOORVCashableWager      AS cashable_coin_in,
    updateWon + updateOORVWon        AS coin_out,
    updateJackpot + updateOORVJackpot AS jackpot,
    maxCashableRisked               AS budget,
    duration,
    sessionUuid,
    assetNumber                     AS raw_asset,
    devInfoGameTheme               AS raw_theme

  FROM {{ env_var('SINGLESTORE_DB') }}.session_data

  WHERE type = 'STUpdate'
    AND playerCardNumber != ''

  {% if is_incremental() %}
    AND FROM_UNIXTIME(eventTime/1000)
        >= (SELECT MAX(hour_bucket) FROM {{ this }})
  {% endif %}
),

-----------------------------------------------------------------------------------
-- 2) Core KPIs per player/hour
-----------------------------------------------------------------------------------
player_agg AS (
  SELECT
    hour_bucket,
    player_card_number,

    SUM(coin_in)                   AS total_coin_in,
    SUM(cashable_coin_in)          AS total_cashable_coin_in,
    SUM(coin_out)                  AS total_coin_out,
    SUM(jackpot)                   AS total_jackpot,
    SUM(coin_in) - SUM(coin_out) - SUM(jackpot) AS total_net_win,
    MAX(budget)                    AS max_player_budget,

    AVG(coin_in)                   AS avg_bet_size,
    COUNT(*)                       AS total_spins,
    COUNT(DISTINCT sessionUuid)    AS total_session_counts,
    COUNT(DISTINCT raw_asset)      AS total_distinct_asset_counts,

    SUM(duration)                  AS total_duration,
    AVG(duration)                  AS avg_duration,
    COUNT(DISTINCT raw_theme)                                  AS distinct_game_themes,
    GROUP_CONCAT(DISTINCT raw_theme ORDER BY raw_theme SEPARATOR ', ')
      AS game_theme_list

  FROM source
  GROUP BY hour_bucket, player_card_number
),

---------------------------------------------------------------------------------------------------
-- 4) Final output: join core KPIs with theme summary
---------------------------------------------------------------------------------------------------
SELECT
  pa.*
FROM player_agg pa;
