"""
Module: test_map_operator.py
Description: MapOperator类的单元测试。
"""

import unittest
from unittest.mock import Mock
from core.data_packet import DataPacket
from operators.map_operator import MapOperator, SimpleMapOperator

class TestMapOperator(unittest.TestCase):
    """
    测试MapOperator类的基本功能。
    """
    
    def setUp(self):
        """
        测试前的准备工作。
        """
        self.test_value = {"input": 1}
        self.test_packet = DataPacket(
            timestamp=1000,
            watermark=900,
            key="test_key",
            value=self.test_value
        )
        
        # 简单的映射函数：将输入值加1
        def add_one(value):
            return {"output": value["input"] + 1}
            
        self.operator = MapOperator({
            'operator_id': 'test_map',
            'map_function': add_one
        })
        
        # 模拟下游算子
        self.mock_downstream = Mock()
        self.operator.add_downstream(self.mock_downstream)

    def test_process(self):
        """
        测试基本的映射功能。
        """
        result = self.operator.process(self.test_packet)
        
        # 验证结果
        self.assertIsNotNone(result)
        self.assertEqual(result.value, {"output": 2})
        self.assertEqual(result.key, self.test_packet.key)  # 默认保留key
        
        # 验证下游调用
        self.mock_downstream.process.assert_called_once()
        called_packet = self.mock_downstream.process.call_args[0][0]
        self.assertEqual(called_packet.value, {"output": 2})

    def test_process_invalid_input(self):
        """
        测试无效输入的处理。
        """
        with self.assertRaises(TypeError):
            self.operator.process("not a packet")

    def test_string_map_function(self):
        """
        测试使用字符串定义的映射函数。
        """
        operator = MapOperator({
            'operator_id': 'test_map',
            'map_function': 'lambda x: {"output": x["input"] * 2}'
        })
        
        result = operator.process(self.test_packet)
        self.assertEqual(result.value, {"output": 2})

    def test_preserve_key_option(self):
        """
        测试key保留选项。
        """
        operator = MapOperator({
            'operator_id': 'test_map',
            'map_function': lambda x: "new_value",
            'preserve_key': False
        })
        
        result = operator.process(self.test_packet)
        self.assertEqual(result.key, "new_value")

class TestSimpleMapOperator(unittest.TestCase):
    """
    测试SimpleMapOperator类的预定义映射函数。
    """
    
    def setUp(self):
        """
        测试前的准备工作。
        """
        self.test_packet = DataPacket(
            timestamp=1000,
            watermark=900,
            key="test_key",
            value={"field1": "value1", "field2": "value2"}
        )

    def test_identity_function(self):
        """
        测试身份映射函数。
        """
        operator = SimpleMapOperator({
            'operator_id': 'test_map',
            'map_function': 'identity'
        })
        
        result = operator.process(self.test_packet)
        self.assertEqual(result.value, self.test_packet.value)

    def test_to_string_function(self):
        """
        测试转字符串映射函数。
        """
        operator = SimpleMapOperator({
            'operator_id': 'test_map',
            'map_function': 'to_string'
        })
        
        result = operator.process(self.test_packet)
        self.assertTrue(isinstance(result.value, str))

    def test_extract_field_function(self):
        """
        测试字段提取映射函数。
        """
        operator = SimpleMapOperator({
            'operator_id': 'test_map',
            'map_function': {'extract_field': 'field1'}
        })
        
        result = operator.process(self.test_packet)
        self.assertEqual(result.value, "value1")

if __name__ == '__main__':
    unittest.main() 