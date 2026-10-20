-- 文件路径: sql/to_mysql.sql
CREATE TABLE IF NOT EXISTS mysql_funnel USING org.apache.spark.sql.jdbc OPTIONS (url 'jdbc:mysql://localhost:3306/ecommerce_ads?useSSL=false&characterEncoding=utf8', dbtable 'ads_funnel_daily', user 'root', password '123456');
INSERT OVERWRITE TABLE mysql_funnel SELECT dt, pv, uv, cart_cnt, fav_cnt, buy_cnt, pv_to_cart_rate, pv_to_buy_rate FROM user_behavior.ads_funnel_daily;

CREATE TABLE IF NOT EXISTS mysql_topn USING org.apache.spark.sql.jdbc OPTIONS (url 'jdbc:mysql://localhost:3306/ecommerce_ads?useSSL=false&characterEncoding=utf8', dbtable 'ads_topn_item_daily', user 'root', password '123456');
INSERT OVERWRITE TABLE mysql_topn SELECT dt, item_id, buy_cnt, `rank` FROM user_behavior.ads_topn_item_daily;

CREATE TABLE IF NOT EXISTS mysql_repurchase USING org.apache.spark.sql.jdbc OPTIONS (url 'jdbc:mysql://localhost:3306/ecommerce_ads?useSSL=false&characterEncoding=utf8', dbtable 'ads_repurchase_rate', user 'root', password '123456');
INSERT OVERWRITE TABLE mysql_repurchase SELECT stat_period, total_buyers, repurchase_users, repurchase_rate FROM user_behavior.ads_repurchase_rate;