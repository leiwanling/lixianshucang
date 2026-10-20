USE user_behavior;

INSERT OVERWRITE TABLE ads_funnel_daily
SELECT dt, pv_cnt, uv, cart_cnt, fav_cnt, buy_cnt,
    ROUND(CAST(cart_cnt AS DOUBLE) / pv_cnt, 4), ROUND(CAST(buy_cnt AS DOUBLE) / pv_cnt, 4)
FROM dws_behavior_daily WHERE pv_cnt > 0;

INSERT OVERWRITE TABLE ads_topn_item_daily
SELECT dt, item_id, buy_cnt, `rank` FROM (
    SELECT dt, item_id, buy_cnt, ROW_NUMBER() OVER (PARTITION BY dt ORDER BY buy_cnt DESC) AS `rank`
    FROM dws_item_daily WHERE buy_cnt > 0
) t WHERE `rank` <= 10;

INSERT OVERWRITE TABLE ads_repurchase_rate
SELECT '2017-11-25 ~ 2017-12-03', COUNT(*), SUM(IF(buy_days >= 2, 1, 0)),
       ROUND(CAST(SUM(IF(buy_days >= 2, 1, 0)) AS DOUBLE) / COUNT(*), 4)
FROM (
    SELECT user_id, COUNT(DISTINCT dt) AS buy_days 
    FROM dwd_user_behavior
    WHERE behavior = 'buy' AND dt != '2017-11-24'
    GROUP BY user_id
) t;