"""
Module: test_keyby_operator.py
Description: KeyByOperator类的单元测试。
"""

import unittest
from unittest.mock import Mock
from typing import Any, Dict
from core.data_packet import DataPacket
from operators.keyby_operator import KeyByOperator

class TestKeyByOperator(unittest.TestCase):
    """
    测试KeyByOperator类的基本功能。
    """
    
    def setUp(self):
        """
        测试前的准备工作。
        """
        # 创建测试数据
        self.test_data = [
            {'user_id': 'user1', 'score': 100},
            {'user_id': 'user2', 'score': 200},
            {'user_id': 'user1', 'score': 150}
        ]
        
        # 创建测试数据包
        self.test_packets = [
            DataPacket(
                timestamp=1000 + i,
                watermark=900 + i,
                key='original_key',
                value=data
            ) for i, data in enumerate(self.test_data)
        ]
        
        # 创建KeyByOperator
        def user_id_selector(value: Dict) -> str:
            return value['user_id']
            
        self.operator = KeyByOperator({
            'operator_id': 'test_keyby',
            'key_selector': user_id_selector,
            'num_partitions': 4
        })
        
        # 模拟下游算子
        self.mock_downstream = Mock()
        self.operator.add_downstream(self.mock_downstream)

    def test_key_selection(self):
        """
        测试key选择功能。
        """
        # 处理第一个数据包
        result = self.operator.process(self.test_packets[0])
        
        # 验证key被正确选择
        self.assertEqual(result.key, 'user1')
        self.assertEqual(result.value, self.test_data[0])
        
        # 验证下游调用
        self.mock_downstream.process.assert_called_once()
        called_packet = self.mock_downstream.process.call_args[0][0]
        self.assertEqual(called_packet.key, 'user1')

    def test_partitioning(self):
        """
        测试分区功能。
        """
        # 记录每个key被分配到的分区
        key_partitions = {}
        
        # 处理所有数据包
        for packet in self.test_packets:
            self.operator.process(packet)
            key = self.operator._key_selector(packet.value)
            partition = self.operator.partition_function(key)
            
            # 验证分区号在有效范围内
            self.assertGreaterEqual(partition, 0)
            self.assertLess(partition, self.operator.num_partitions)
            
            # 验证相同的key总是被分配到相同的分区
            if key in key_partitions:
                self.assertEqual(partition, key_partitions[key])
            else:
                key_partitions[key] = partition

    def test_string_key_selector(self):
        """
        测试使用字符串字段名作为key选择器。
        """
        operator = KeyByOperator({
            'operator_id': 'test_keyby',
            'key_selector': 'user_id',  # 直接使用字段名
            'num_partitions': 4
        })
        
        result = operator.process(self.test_packets[0])
        self.assertEqual(result.key, 'user1')

    def test_custom_partition_function(self):
        """
        测试自定义分区函数。
        """
        def custom_partition(key: str) -> int:
            return len(key) % 4
            
        operator = KeyByOperator({
            'operator_id': 'test_keyby',
            'key_selector': 'user_id',
            'num_partitions': 4,
            'partition_function': custom_partition
        })
        
        # 处理数据并验证分区结果
        result = operator.process(self.test_packets[0])
        partition = operator.partition_function(result.key)
        self.assertEqual(partition, len('user1') % 4)

    def test_invalid_key_selector(self):
        """
        测试无效的key选择器。
        """
        # 测试不存在的字段名
        operator = KeyByOperator({
            'operator_id': 'test_keyby',
            'key_selector': 'non_existent_field',
            'num_partitions': 4
        })
        
        # 应该返回None而不是抛出异常
        result = operator.process(self.test_packets[0])
        self.assertIsNone(result)

    def test_error_handling(self):
        """
        测试错误处理。
        """
        # 创建一个会抛出异常的key选择器
        def error_selector(_):
            raise ValueError("测试错误")
            
        operator = KeyByOperator({
            'operator_id': 'test_keyby',
            'key_selector': error_selector,
            'num_partitions': 4
        })
        
        # 验证错误被正确处理
        with self.assertLogs(level='ERROR') as log:
            result = operator.process(self.test_packets[0])
            
            self.assertIsNone(result)
            self.assertTrue(any('测试错误' in record.message 
                              for record in log.records))

    def test_initialization_validation(self):
        """
        测试初始化参数验证。
        """
        # 测试缺少key_selector
        with self.assertRaises(ValueError):
            KeyByOperator({
                'operator_id': 'test_keyby',
                'num_partitions': 4
            })
        
        # 测试无效的分区数
        with self.assertRaises(ValueError):
            KeyByOperator({
                'operator_id': 'test_keyby',
                'key_selector': 'user_id',
                'num_partitions': 0
            })

    def tearDown(self):
        """
        测试后的清理工作。
        """
        self.operator.close()

if __name__ == '__main__':
    unittest.main() 