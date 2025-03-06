"""
Module: test_kafka_sink_operator.py
Description: KafkaSinkOperator类的单元测试。
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, List
import json
import time
from core.data_packet import DataPacket
from operators.sink_operator import KafkaSinkOperator

class TestKafkaSinkOperator(unittest.TestCase):
    """
    测试KafkaSinkOperator类的基本功能。
    """
    
    def setUp(self):
        """
        测试前的准备工作。
        """
        # 准备测试数据包
        self.test_packets = [
            DataPacket(
                timestamp=1000,
                watermark=900,
                key='key1',
                value={'data': 'message1'}
            ),
            DataPacket(
                timestamp=2000,
                watermark=1900,
                key='key2',
                value={'data': 'message2'}
            )
        ]
        
        # 创建配置
        self.config = {
            'operator_id': 'test_kafka_sink',
            'bootstrap_servers': 'localhost:9092',
            'topic': 'test_topic',
            'batch_size': 2,
            'producer_config': {
                'compression_type': 'gzip',
                'linger_ms': 100
            }
        }

    @patch('kafka.KafkaProducer')
    def test_basic_production(self, mock_kafka_producer):
        """
        测试基本的消息生产功能。
        """
        # 配置Mock生产者
        mock_producer = MagicMock()
        mock_producer.send.return_value.get.return_value = None
        mock_kafka_producer.return_value = mock_producer
        
        # 创建Sink算子
        operator = KafkaSinkOperator(self.config)
        operator.initialize()
        
        # 发送数据包
        for packet in self.test_packets:
            operator.process(packet)
            
        # 验证生产者调用
        self.assertEqual(
            mock_producer.send.call_count,
            len(self.test_packets)
        )
        
        # 验证消息内容
        for i, call in enumerate(mock_producer.send.call_args_list):
            args, kwargs = call
            topic = args[0]
            value = json.loads(kwargs['value'].decode('utf-8'))
            key = kwargs['key'].decode('utf-8')
            
            self.assertEqual(topic, 'test_topic')
            self.assertEqual(key, self.test_packets[i].key)
            self.assertEqual(value['data'], self.test_packets[i].value['data'])

    @patch('kafka.KafkaProducer')
    def test_producer_config(self, mock_kafka_producer):
        """
        测试Kafka生产者配置。
        """
        operator = KafkaSinkOperator(self.config)
        operator.initialize()
        
        # 验证生产者创建参数
        mock_kafka_producer.assert_called_once()
        _, kwargs = mock_kafka_producer.call_args
        self.assertEqual(kwargs['bootstrap_servers'], 'localhost:9092')
        self.assertEqual(kwargs['compression_type'], 'gzip')
        self.assertEqual(kwargs['linger_ms'], 100)

    @patch('kafka.KafkaProducer')
    def test_error_handling(self, mock_kafka_producer):
        """
        测试错误处理。
        """
        # 模拟Kafka连接错误
        mock_kafka_producer.side_effect = Exception("Kafka连接失败")
        
        # 验证初始化错误处理
        with self.assertLogs(level='ERROR') as log:
            operator = KafkaSinkOperator(self.config)
            with self.assertRaises(RuntimeError):
                operator.initialize()
            
            self.assertTrue(any('Kafka连接失败' in record.message 
                              for record in log.records))

    @patch('kafka.KafkaProducer')
    def test_batch_processing(self, mock_kafka_producer):
        """
        测试批量处理。
        """
        # 配置Mock生产者
        mock_producer = MagicMock()
        mock_producer.send.return_value.get.return_value = None
        mock_kafka_producer.return_value = mock_producer
        
        # 创建小批量的Sink算子
        self.config['batch_size'] = 2
        operator = KafkaSinkOperator(self.config)
        operator.initialize()
        
        # 发送数据包
        for packet in self.test_packets:
            operator.process(packet)
        
        # 验证批量发送
        self.assertEqual(mock_producer.send.call_count, 1)
        
        # 验证最后一批数据被刷新
        operator.close()
        self.assertEqual(mock_producer.flush.call_count, 1)

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
                KafkaSinkOperator(config)

    def tearDown(self):
        """
        测试后的清理工作。
        """
        pass

if __name__ == '__main__':
    unittest.main() 