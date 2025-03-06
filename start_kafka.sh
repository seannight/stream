#!/bin/bash

# 启动Kafka环境
docker-compose up -d

# 等待Kafka启动
echo "Waiting for Kafka to start..."
sleep 10

# 创建测试主题
docker-compose exec kafka kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic test_topic

# 显示主题列表
docker-compose exec kafka kafka-topics.sh \
    --list \
    --bootstrap-server localhost:9092

echo "Kafka environment is ready!" 