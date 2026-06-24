#!/bin/bash
PROJECT_DIR=/home/hadoop/workspace/SparkKafkaDashboard
KAFKA_HOME=/home/hadoop/kafka

echo "Stopping SparkKafkaDashboard..."

# 1. 停止 Python 进程 + Spark
for proc in "producer.py" "app.py" "SparkSubmit"; do
    pid=$(ps aux | grep "$proc" | grep -v grep | awk '{print $2}')
    if [ -n "$pid" ]; then
        kill $pid 2>/dev/null
        echo "[OK] $proc stopped (PID $pid)"
    else
        echo "[--] $proc not running"
    fi
done

# 2. 停止 Kafka
pid=$(ps aux | grep "kafka.Kafka" | grep -v grep | awk '{print $2}')
if [ -n "$pid" ]; then
    $KAFKA_HOME/bin/kafka-server-stop.sh > /dev/null 2>&1
    # 等 5 秒，若未停止则强制结束
    sleep 3
    if ps aux | grep "kafka.Kafka" | grep -v grep > /dev/null 2>&1; then
        kill -9 $pid 2>/dev/null
        echo "[OK] Kafka force killed (PID $pid)"
    else
        echo "[OK] Kafka stopped gracefully"
    fi
else
    echo "[--] Kafka not running"
fi

echo ""
echo "All services stopped."
