"""
Module: test_kafka_source_operator.py
Description: KafkaSourceOperator类的单元测试。
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, List
import json
import time
from core.data_packet import DataPacket
from operators.source_operator import KafkaSourceOperator

class TestKafkaSourceOperator(unittest.TestCase):
    """
    测试KafkaSourceOperator类的基本功能。
    """
    
    def setUp(self):
        """
        测试前的准备工作。
        """
        # 准备测试消息
        self.test_messages = [
            {
                'timestamp': 1000,
                'key': 'key1',
                'value': {'data': 'message1'}
            },
            {
                'timestamp': 2000,
                'key': 'key2',
                'value': {'data': 'message2'}
            }
        ]
        
        # 创建配置
        self.config = {
            'operator_id': 'test_kafka_source',
            'bootstrap_servers': 'localhost:9092',
            'topic': 'test_topic',
            'group_id': 'test_group',
            'auto_offset_reset': 'earliest'
        }
        
        # 模拟下游算子
        self.mock_downstream = Mock()

    @patch('kafka.KafkaConsumer')
    def test_basic_consumption(self, mock_kafka_consumer):
        """
        测试基本的消息消费功能。
        """
        # 配置Mock消费者
        mock_consumer = MagicMock()
        mock_consumer.__iter__.return_value = [
            Mock(
                value=json.dumps(msg).encode('utf-8'),
                key=msg['key'].encode('utf-8'),
                timestamp=(1, msg['timestamp'])
            ) for msg in self.test_messages
        ]
        mock_kafka_consumer.return_value = mock_consumer
        
        # 创建源算子
        operator = KafkaSourceOperator(self.config)
        operator.add_downstream(self.mock_downstream)
        
        # 处理消息
        operator.initialize()
        for _ in range(len(self.test_messages)):
            operator.process(None)  # Source算子忽略输入
            
        # 验证下游调用
        self.assertEqual(
            self.mock_downstream.process.call_count,
            len(self.test_messages)
        )
        
        # 验证消息内容
        for i, call in enumerate(self.mock_downstream.process.call_args_list):
            packet = call[0][0]
            self.assertIsInstance(packet, DataPacket)
            self.assertEqual(packet.timestamp, self.test_messages[i]['timestamp'])
            self.assertEqual(packet.key, self.test_messages[i]['key'])
            self.assertEqual(packet.value, self.test_messages[i]['value'])

    @patch('kafka.KafkaConsumer')
    def test_consumer_config(self, mock_kafka_consumer):
        """
        测试Kafka消费者配置。
        """
        # 添加额外的消费者配置
        self.config.update({
            'consumer_config': {
                'max_poll_records': 100,
                'session_timeout_ms': 30000
            }
        })
        
        # 创建源算子
        operator = KafkaSourceOperator(self.config)
        operator.initialize()
        
        # 验证消费者创建参数
        mock_kafka_consumer.assert_called_once()
        _, kwargs = mock_kafka_consumer.call_args
        self.assertEqual(kwargs['bootstrap_servers'], 'localhost:9092')
        self.assertEqual(kwargs['group_id'], 'test_group')
        self.assertEqual(kwargs['max_poll_records'], 100)
        self.assertEqual(kwargs['session_timeout_ms'], 30000)

    @patch('kafka.KafkaConsumer')
    def test_error_handling(self, mock_kafka_consumer):
        """
        测试错误处理。
        """
        # 模拟Kafka连接错误
        mock_kafka_consumer.side_effect = Exception("Kafka连接失败")
        
        # 验证初始化错误处理
        with self.assertLogs(level='ERROR') as log:
            operator = KafkaSourceOperator(self.config)
            with self.assertRaises(RuntimeError):
                operator.initialize()
            
            self.assertTrue(any('Kafka连接失败' in record.message 
                              for record in log.records))

    @patch('kafka.KafkaConsumer')
    def test_message_deserialization(self, mock_kafka_consumer):
        """
        测试消息反序列化。
        """
        # 配置Mock消费者返回无效消息
        mock_consumer = MagicMock()
        mock_consumer.__iter__.return_value = [
            Mock(
                value=b'invalid json',
                key=None,
                timestamp=(1, 1000)
            )
        ]
        mock_kafka_consumer.return_value = mock_consumer
        
        # 创建源算子
        operator = KafkaSourceOperator(self.config)
        operator.add_downstream(self.mock_downstream)
        
        # 处理消息
        operator.initialize()
        with self.assertLogs(level='ERROR') as log:
            operator.process(None)
            
            # 验证错误被记录
            self.assertTrue(any('反序列化失败' in record.message 
                              for record in log.records))
            
            # 验证下游没有被调用
            self.mock_downstream.process.assert_not_called()

    def test_initialization_validation(self):
        """
        测试初始化参数验证。
        """
        # 测试缺少必需参数
        invalid_configs = [
            {'operator_id': 'test'},  # 缺少bootstrap_servers
            {
                'operator_id': 'test',
                'bootstrap_servers': 'localhost:9092'
            },  # 缺少topic
            {
                'operator_id': 'test',
                'bootstrap_servers': 'localhost:9092',
                'topic': ''
            }  # 空topic
        ]
        
        for config in invalid_configs:
            with self.assertRaises(ValueError):
                KafkaSourceOperator(config)

    def tearDown(self):
        """
        测试后的清理工作。
        """
        pass

if __name__ == '__main__':
    unittest.main() 