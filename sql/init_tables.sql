-- ==========================================
-- 数仓初始化建表脚本 (仅需执行一次)
-- ==========================================
CREATE DATABASE IF NOT EXISTS user_behavior;
USE user_behavior;

-- 1. DWD 层明细表
CREATE TABLE IF NOT EXISTS dwd_user_behavior (
    user_id BIGINT, item_id BIGINT, category_id BIGINT, behavior STRING, ts BIGINT, hour STRING
) PARTITIONED BY (dt STRING) STORED AS PARQUET;

-- 2. DWS 层汇总表
CREATE TABLE IF NOT EXISTS dws_behavior_daily(
    pv_cnt BIGINT, cart_cnt BIGINT, fav_cnt BIGINT, buy_cnt BIGINT, uv BIGINT
) PARTITIONED BY (dt STRING) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS dws_item_daily(
    item_id BIGINT, pv_cnt BIGINT, cart_cnt BIGINT, fav_cnt BIGINT, buy_cnt BIGINT
) PARTITIONED BY (dt STRING) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS dws_user_daily(
    user_id BIGINT, pv_cnt BIGINT, cart_cnt BIGINT, fav_cnt BIGINT, buy_cnt BIGINT
) PARTITIONED BY (dt STRING) STORED AS PARQUET;

-- 3. ADS 层指标表
CREATE TABLE IF NOT EXISTS ads_funnel_daily(
    dt STRING, pv BIGINT, uv BIGINT, cart_cnt BIGINT, fav_cnt BIGINT, buy_cnt BIGINT, 
    pv_to_cart_rate DOUBLE, pv_to_buy_rate DOUBLE
) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ads_topn_item_daily(
    dt STRING, item_id BIGINT, buy_cnt BIGINT, `rank` INT
) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS ads_repurchase_rate(
    stat_period STRING, total_buyers BIGINT, repurchase_users BIGINT, repurchase_rate DOUBLE
) STORED AS PARQUET;