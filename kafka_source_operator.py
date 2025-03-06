"""
Kafka source operator implementation.
"""
from typing import Any, Dict
from kafka import KafkaConsumer
import json

class KafkaSourceOperator:
    """
    Source operator that reads data from Kafka.
    """
    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Initialize the Kafka source operator.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic to consume from
        """
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.operator_id = None  # 初始化 operator_id 为 None