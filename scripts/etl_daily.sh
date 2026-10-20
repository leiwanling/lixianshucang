#!/bin/bash
echo "================ 任务开始 $(date) ================"

export SPARK_HOME=/usr/local/spark
SQL_DIR=/root/sql
LOG_DIR=/root/scripts/logs
mkdir -p $LOG_DIR

# 加入 Spark AQE 自动倾斜处理参数
SPARK_SUBMIT="$SPARK_HOME/bin/spark-sql --master local[*] --driver-memory 1g \
--conf spark.sql.shuffle.partitions=8 \
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.skewJoin.enabled=true \
--conf spark.sql.adaptive.coalescePartitions.enabled=true"

echo "[1/4] 执行 DWD 层..."
$SPARK_SUBMIT -f $SQL_DIR/dwd.sql > $LOG_DIR/dwd.log 2>&1
if [ $? -eq 0 ]; then echo " -> DWD层执行成功"; else echo " -> DWD层失败，看日志!"; exit 1; fi

echo "[2/4] 执行 DWS 层..."
$SPARK_SUBMIT -f $SQL_DIR/dws.sql > $LOG_DIR/dws.log 2>&1
if [ $? -eq 0 ]; then echo " -> DWS层执行成功"; else echo " -> DWS层失败，看日志!"; exit 1; fi

echo "[3/4] 执行 ADS 层..."
$SPARK_SUBMIT -f $SQL_DIR/ads.sql > $LOG_DIR/ads.log 2>&1
if [ $? -eq 0 ]; then echo " -> ADS层执行成功"; else echo " -> ADS层失败，看日志!"; exit 1; fi

echo "[4/4] 写入 MySQL..."
$SPARK_SUBMIT -f $SQL_DIR/to_mysql.sql > $LOG_DIR/mysql.log 2>&1
if [ $? -eq 0 ]; then echo " -> MySQL导出成功"; else echo " -> MySQL导出失败，看日志!"; exit 1; fi

echo "================ 任务结束 $(date) ================"