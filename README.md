# 淘宝双11实时分析 Dashboard (Taobao Real-time Dashboard)

基于 Kafka + Spark Structured Streaming + Flask + ECharts 的实时数据看板。

## 项目概述

本项目基于厦门大学数据库实验室《Spark+Kafka构建实时分析Dashboard》案例，对淘宝用户行为数据进行实时流式处理与可视化展示。数据从 CSV 出发，经 Kafka 消息队列传输，由 Spark Structured Streaming 实时聚合，最终通过 Flask REST API 推送至前端 ECharts 图表。

## 系统架构

```
user_log.csv → producer.py → Kafka[sex] → Spark Streaming → Kafka[result] → Flask /api/data → ECharts
```

## 技术栈

| 技术 | 用途 |
|------|------|
| Ubuntu 24.04 | 操作系统 |
| Kafka 3.9.2 | 消息队列（Kraft 模式） |
| Spark 3.5.8 | Structured Streaming 实时处理 |
| Flask 2.3.3 | Python Web 框架 |
| ECharts 5.x | 前端可视化库 |
| Python 3.12 | 脚本开发 |

## 数据流

1. **Producer** — 读取 `user_log.csv`，提取 gender 字段（0=女, 1=男, 2=未知），每 0.1 秒发送一条到 Kafka topic `sex`
2. **Spark Streaming** — 消费 topic `sex`，3 秒滑动窗口聚合，`withWatermark` 处理延迟数据，写入 topic `result`
3. **Flask** — 后台线程消费 topic `result`，通过 REST API `/api/data` 暴露数据
4. **ECharts** — 每 1 秒 fetch 轮询，实时更新折线图（30 点滑动窗口）

## 项目结构

```
SparkKafkaDashboard/
├── app.py                          # Flask REST API
├── scripts/
│   └── producer.py                 # Kafka 生产者
├── streaming/
│   └── spark_streaming.py          # Spark Structured Streaming
├── templates/
│   └── index.html                  # ECharts 实时折线图
├── static/js/
│   ├── echarts.min.js
│   └── socket.io.min.js
├── data/
│   └── user_log.csv                # 10000 行测试数据集
├── start.sh                        # 一键启动脚本
└── stop.sh                         # 一键停止脚本
```

## 快速开始

### 环境要求

- JDK 17+
- Kafka 3.x
- Spark 3.x
- Python 3.10+
- kafka-python 3.x
- Flask 2.x

### 启动

```bash
# 一键启动
cd SparkKafkaDashboard
bash start.sh
```

### 停止

```bash
bash stop.sh
```

### 访问

浏览器打开 `http://localhost:5000`

## 参考

- [厦大数据库实验室 - Spark+Kafka实时分析Dashboard](https://dblab.xmu.edu.cn/post/spark-kafka-dashboard/)
