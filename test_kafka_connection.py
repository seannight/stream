"""
Module: test_kafka_connection.py
Description: 测试Kafka连接是否正常，包括生产者和消费者连接测试。
"""

import logging
import sys
from kafka import KafkaProducer, KafkaConsumer
import json
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_kafka_connection(bootstrap_servers='localhost:9092', test_topic='test_topic'):
    """
    测试Kafka连接，包括生产者和消费者连接测试。
    
    @param bootstrap_servers (str): Kafka服务器地址，默认为localhost:9092
    @param test_topic (str): 测试主题名称，默认为test_topic
    @returns (bool): 连接测试是否成功
    """
    producer_success = False
    consumer_success = False
    
    # 测试生产者连接
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=str.encode
        )
        producer.close()
        logger.info(f"Kafka生产者连接成功: {bootstrap_servers}")
        producer_success = True
    except Exception as e:
        logger.error(f"Kafka生产者连接失败: {e}")
    
    # 测试消费者连接
    try:
        consumer = KafkaConsumer(
            test_topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            group_id='test-connection-group',
            consumer_timeout_ms=5000  # 5秒超时
        )
        consumer.close()
        logger.info(f"Kafka消费者连接成功: {bootstrap_servers}, 主题: {test_topic}")
        consumer_success = True
    except Exception as e:
        logger.error(f"Kafka消费者连接失败: {e}")
    
    # 返回测试结果
    if producer_success and consumer_success:
        logger.info("Kafka连接测试全部通过")
        return True
    else:
        logger.error("Kafka连接测试失败")
        return False

if __name__ == "__main__":
    success = test_kafka_connection()
    sys.exit(0 if success else 1) 