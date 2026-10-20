USE user_behavior;
SET spark.sql.shuffle.partitions=8;
SET spark.sql.adaptive.enabled=true;

INSERT OVERWRITE TABLE dwd_user_behavior PARTITION(dt)
SELECT 
    CAST(user_id AS BIGINT), CAST(item_id AS BIGINT), CAST(category_id AS BIGINT), behavior,
    CAST(`timestamp` AS BIGINT) AS ts,
    date_format(from_unixtime(CAST(`timestamp` AS BIGINT)), 'HH') AS hour,
    to_date(from_unixtime(CAST(`timestamp` AS BIGINT))) AS dt
FROM default.ods_user_behavior
WHERE CAST(`timestamp` AS BIGINT) >= 1511544070 
  AND CAST(`timestamp` AS BIGINT) <= 1512312365;