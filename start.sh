#!/bin/bash
PROJECT_DIR=/home/hadoop/workspace/SparkKafkaDashboard
KAFKA_HOME=/home/hadoop/kafka
VENV_PYTHON=/home/hadoop/venv_sparkweb/bin/python3
LOG_DIR=$PROJECT_DIR/logs

mkdir -p $LOG_DIR

# ── 1. 初始化 Kafka Kraft 元数据（如丢失） ──
META_DIR=/tmp/kraft-combined-logs
if [ ! -f "$META_DIR/__cluster_metadata-0/00000000000000000000.log" ]; then
    echo "[...] Initializing Kafka Kraft metadata..."
    rm -rf $META_DIR
    CLUSTER_ID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
    $KAFKA_HOME/bin/kafka-storage.sh format -t $CLUSTER_ID -c $KAFKA_HOME/config/kraft/server.properties > /dev/null 2>&1
    echo "[OK] Kraft metadata initialized"
fi

# ── 2. 启动 Kafka ──
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/kraft/server.properties > /dev/null 2>&1
echo "[...] Waiting for Kafka to start..."

# 等待 Kafka 就绪（最多 30 秒）
for i in $(seq 1 30); do
    $KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "[OK] Kafka ready"
        break
    fi
    sleep 1
done

# ── 3. 创建 topic（如不存在） ──
for topic in sex result; do
    $KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 2>/dev/null | grep -q "^$topic$"
    if [ $? -ne 0 ]; then
        $KAFKA_HOME/bin/kafka-topics.sh --create --topic $topic \
            --bootstrap-server localhost:9092 \
            --partitions 1 --replication-factor 1 > /dev/null 2>&1
        echo "[OK] Topic '$topic' created"
    else
        echo "[OK] Topic '$topic' exists"
    fi
done

# ── 4. 启动 Producer ──
$VENV_PYTHON $PROJECT_DIR/scripts/producer.py > $LOG_DIR/producer.log 2>&1 &
echo "[OK] Producer started ($!)"

# ── 5. 启动 Spark Structured Streaming ──
nohup /usr/local/spark/bin/spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  --conf spark.pyspark.python=$VENV_PYTHON \
  $PROJECT_DIR/streaming/spark_streaming.py > $LOG_DIR/streaming.log 2>&1 &
echo "[OK] Spark Streaming started ($!)"

# ── 6. 启动 Flask ──
$VENV_PYTHON $PROJECT_DIR/app.py > $LOG_DIR/app.log 2>&1 &
echo "[OK] Flask started ($!)"

echo ""
echo "========================"
echo "  Dashboard ready!"
echo "  http://localhost:5000"
echo "  http://$(hostname -I | awk '{print $1}'):5000"
echo "  Logs: $LOG_DIR"
echo "========================"
