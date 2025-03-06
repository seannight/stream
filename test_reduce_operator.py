"""
Module: test_reduce_operator.py
Description: ReduceOperator类的单元测试。
"""

import unittest
from unittest.mock import Mock, patch
from typing import Any, Dict
from core.data_packet import DataPacket
from core.state import MemoryStateBackend
from operators.reduce_operator import ReduceOperator

class TestReduceOperator(unittest.TestCase):
    """
    测试ReduceOperator类的基本功能。
    """
    
    def setUp(self):
        """
        测试前的准备工作。
        """
        # 创建测试数据
        self.test_data = [
            {'user_id': 'user1', 'score': 100},
            {'user_id': 'user1', 'score': 150},
            {'user_id': 'user2', 'score': 200}
        ]
        
        # 创建测试数据包
        self.test_packets = [
            DataPacket(
                timestamp=1000 + i,
                watermark=900 + i,
                key=data['user_id'],
                value=data
            ) for i, data in enumerate(self.test_data)
        ]
        
        # 定义聚合函数：计算分数总和
        def sum_scores(acc: Dict[str, Any], value: Dict[str, Any]) -> Dict[str, Any]:
            return {
                'user_id': value['user_id'],
                'total_score': acc.get('total_score', 0) + value['score']
            }
            
        # 创建ReduceOperator
        self.operator = ReduceOperator({
            'operator_id': 'test_reduce',
            'reduce_function': sum_scores,
            'state_backend': {'type': 'memory'}
        })
        
        # 模拟下游算子
        self.mock_downstream = Mock()
        self.operator.add_downstream(self.mock_downstream)

    def test_basic_reduce(self):
        """
        测试基本的聚合功能。
        """
        # 处理第一个数据包
        result1 = self.operator.process(self.test_packets[0])
        self.assertEqual(result1.value['total_score'], 100)
        
        # 处理第二个数据包（同一个key）
        result2 = self.operator.process(self.test_packets[1])
        self.assertEqual(result2.value['total_score'], 250)  # 100 + 150
        
        # 验证下游调用
        self.assertEqual(self.mock_downstream.process.call_count, 2)

    def test_multiple_keys(self):
        """
        测试多个key的聚合。
        """
        # 处理所有数据包
        for packet in self.test_packets:
            self.operator.process(packet)
        
        # 获取最终状态
        state = self.operator.state_backend
        
        # 验证每个key的聚合结果
        user1_state = state.load_state('test_reduce', 'user1')
        user2_state = state.load_state('test_reduce', 'user2')
        
        self.assertEqual(user1_state['total_score'], 250)  # 100 + 150
        self.assertEqual(user2_state['total_score'], 200)

    def test_string_reduce_function(self):
        """
        测试使用字符串定义的聚合函数。
        """
        operator = ReduceOperator({
            'operator_id': 'test_reduce',
            'reduce_function': 'lambda acc, val: {"sum": acc.get("sum", 0) + val["score"]}',
            'state_backend': {'type': 'memory'}
        })
        
        result = operator.process(self.test_packets[0])
        self.assertEqual(result.value['sum'], 100)

    def test_state_persistence(self):
        """
        测试状态持久化。
        """
        # 处理第一个数据包
        self.operator.process(self.test_packets[0])
        
        # 模拟重启：创建新的算子实例
        new_operator = ReduceOperator({
            'operator_id': 'test_reduce',
            'reduce_function': self.operator.reduce_function,
            'state_backend': {'type': 'memory'}
        })
        
        # 验证状态恢复
        state = new_operator.state_backend.load_state('test_reduce', 'user1')
        self.assertIsNotNone(state)
        self.assertEqual(state['total_score'], 100)

    def test_error_handling(self):
        """
        测试错误处理。
        """
        # 创建一个会抛出异常的聚合函数
        def error_reducer(_, __):
            raise ValueError("测试错误")
            
        operator = ReduceOperator({
            'operator_id': 'test_reduce',
            'reduce_function': error_reducer,
            'state_backend': {'type': 'memory'}
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
        # 测试缺少reduce_function
        with self.assertRaises(ValueError):
            ReduceOperator({
                'operator_id': 'test_reduce',
                'state_backend': {'type': 'memory'}
            })
        
        # 测试无效的状态后端类型
        with self.assertRaises(ValueError):
            ReduceOperator({
                'operator_id': 'test_reduce',
                'reduce_function': lambda x, y: x + y,
                'state_backend': {'type': 'invalid'}
            })

    def test_state_cleanup(self):
        """
        测试状态清理。
        """
        # 处理一些数据
        for packet in self.test_packets:
            self.operator.process(packet)
        
        # 清理特定key的状态
        self.operator.state_backend.clear_state('test_reduce', 'user1')
        
        # 验证状态被清理
        state = self.operator.state_backend.load_state('test_reduce', 'user1')
        self.assertIsNone(state)

    def tearDown(self):
        """
        测试后的清理工作。
        """
        if hasattr(self, 'operator'):
            self.operator.close()

if __name__ == '__main__':
    unittest.main() 