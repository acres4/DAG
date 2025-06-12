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

    -- lets try total, avg, cnt, max as counts, 

    -- Total Metrics here
    SUM(updateCashableWager + updateRestrictedWager 
      + updateOORVCashableWager + updateOORVRestrictedWager)          AS total_coin_in,
    SUM(updateCashableWager + updateOORVCashableWager)          AS total_cashable_coin_in,
    SUM(updateWon + updateOORVWon) as total_coin_out,
    SUM(updateJackpot + updateOORVJackpot) as total_jackpot,
    total_coin_in-coin_out-total_jackpot as total_net_win,
    max(maxCashableRisked) as max_player_budget, 

    -- avg Metrics
    AVG(updateCashableWager + updateRestrictedWager + updateOORVCashableWager + updateOORVRestrictedWager)          AS total_avg_bet_size,
    -- TODO: avg Bet Velocity is gonna be a little bit higher hanging fruit. 
    -- A little more difficult sql query. I will get back to it. Let me just get 

    -- Counts
    COUNT(*)                          AS total_spins,
    count (distinct sessionUuid) as total_session_counts,
    count (distinct assetNumber) as total_distinct_asset_counts,
    count (distinct devInfoGameThemes) as total_distinct_game_themes
    count (distinct playerCardNumber) as total_distinct_player_card_numbers
    
    
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