{{ config(
    materialized='incremental',
    unique_key=['day_bucket', 'playerCardNumber']
) }}

-- Leaving a note: 
-- TODO: It would be better to refactor these daily codes to so that it 
-- would pull whatever it needed from the hourly metrics 
-- and whatever else it would aggregate from the main table. 
-- The current version just uses the current table and aggregates
-- Leaving for now, with intention getting something working quickly and can fine tune later. 

-- ALSO IMPORTANT TO CLARIFY: ARE WE DOING THIS BY GAMING DATE OR CALENDAR DATE?
-- Assuming calendar date because all over doc.


WITH source AS (
  SELECT
    DATE_FORMAT(FROM_UNIXTIME(eventTime/1000), '%Y-%m-%d 00:00:00')
      AS day_bucket,
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
    AND DATE(FROM_UNIXTIME(eventTime/1000)) 
        >= (SELECT MAX(day_bucket) FROM {{ this }})
  {% endif %}
),

-----------------------------------------------------------------------------------
-- 2) Core KPIs per player/hour
-----------------------------------------------------------------------------------
player_agg AS (
  SELECT
    day_bucket,
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


