USE user_behavior;
SET spark.sql.shuffle.partitions=8;
SET spark.sql.adaptive.enabled=true;

-- 1. 按天汇总 (无倾斜)
INSERT OVERWRITE TABLE dws_behavior_daily PARTITION(dt)
SELECT SUM(IF(behavior='pv',1,0)), SUM(IF(behavior='cart',1,0)), 
       SUM(IF(behavior='fav',1,0)), SUM(IF(behavior='buy',1,0)), 
       COUNT(DISTINCT user_id), dt 
FROM dwd_user_behavior GROUP BY dt;

-- 2. 按商品汇总 (两阶段聚合，解决商品爆款长尾数据倾斜)
-- 第一阶段：在子查询中固定随机盐，打散数据
DROP TABLE IF EXISTS tmp_dws_item_stage1;
CREATE TABLE tmp_dws_item_stage1 USING PARQUET AS
SELECT dt, salt_item_id, 
       SUM(IF(behavior='pv',1,0)) AS pv_cnt,
       SUM(IF(behavior='cart',1,0)) AS cart_cnt,
       SUM(IF(behavior='fav',1,0)) AS fav_cnt,
       SUM(IF(behavior='buy',1,0)) AS buy_cnt
FROM (
    -- 核心修复：在这里生成固定的盐值
    SELECT dt, behavior, CONCAT(CAST(item_id AS STRING), '_', CAST(FLOOR(RAND() * 10) AS STRING)) AS salt_item_id
    FROM dwd_user_behavior
) t
GROUP BY dt, salt_item_id;

-- 第二阶段：去掉盐值（截取下划线前面的部分），全局聚合
INSERT OVERWRITE TABLE dws_item_daily PARTITION(dt)
SELECT CAST(SPLIT(salt_item_id, '_')[0] AS BIGINT) AS item_id,
       SUM(pv_cnt), SUM(cart_cnt), SUM(fav_cnt), SUM(buy_cnt), dt
FROM tmp_dws_item_stage1
GROUP BY dt, CAST(SPLIT(salt_item_id, '_')[0] AS BIGINT);

-- 清理临时表（养成好习惯，防止占用HDFS空间）
DROP TABLE IF EXISTS tmp_dws_item_stage1;

-- 3. 按用户汇总 (无倾斜)
INSERT OVERWRITE TABLE dws_user_daily PARTITION(dt)
SELECT user_id, SUM(IF(behavior='pv',1,0)), SUM(IF(behavior='cart',1,0)), 
       SUM(IF(behavior='fav',1,0)), SUM(IF(behavior='buy',1,0)), dt 
FROM dwd_user_behavior GROUP BY dt, user_id;