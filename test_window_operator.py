"""
Module: test_window_operator.py
Description: WindowOperator类的单元测试。
"""

import unittest
from unittest.mock import Mock
import time
from typing import Any  # 添加 Any 类型导入
from core.data_packet import DataPacket
from operators.window_operator import WindowOperator, Window

class TestWindowOperator(unittest.TestCase):
    """
    测试WindowOperator类的基本功能。
    """
    
    def setUp(self):
        """
        测试前的准备工作。
        """
        # 配置5秒的滚动窗口
        self.window_size_ms = 5000
        self.operator = WindowOperator({
            'operator_id': 'test_window',
            'window_size_ms': self.window_size_ms,
            'max_out_of_orderness': 1000  # 1秒的乱序容忍
        })
        
        # 模拟下游算子
        self.mock_downstream = Mock()
        self.operator.add_downstream(self.mock_downstream)
        
        # 基准时间：2024-01-01 00:00:00
        self.base_time = 1704067200000
        
    def create_packet(self, offset_ms: int, key: str = "test_key", value: Any = None) -> DataPacket:
        """
        创建测试数据包。

        Args:
            offset_ms: 相对于基准时间的偏移（毫秒）
            key: 数据key
            value: 数据值

        Returns:
            DataPacket: 创建的数据包
        """
        return DataPacket(
            timestamp=self.base_time + offset_ms,
            watermark=self.base_time + offset_ms - 1000,  # 默认1秒延迟
            key=key,
            value=value or {"count": 1}
        )

    def test_window_creation(self):
        """
        测试窗口创建。
        """
        packet = self.create_packet(1000)  # 第一个窗口的数据
        self.operator.process(packet)
        
        # 验证窗口创建
        window = self.operator._get_window(packet.key, packet.timestamp)
        self.assertIsNotNone(window)
        self.assertEqual(window.start_time, self.base_time)
        self.assertEqual(window.end_time, self.base_time + self.window_size_ms)
        self.assertEqual(len(window.data), 1)

    def test_window_trigger(self):
        """
        测试窗口触发。
        """
        # 发送第一个窗口的数据
        packet1 = self.create_packet(1000)
        packet2 = self.create_packet(2000)
        self.operator.process(packet1)
        self.operator.process(packet2)
        
        # 发送下一个窗口的数据（会触发第一个窗口）
        next_window_packet = self.create_packet(self.window_size_ms + 2000)
        self.operator.process(next_window_packet)
        
        # 验证下游调用
        self.mock_downstream.process.assert_called_once()
        result = self.mock_downstream.process.call_args[0][0]
        
        # 验证窗口结果
        self.assertEqual(result.key, packet1.key)
        self.assertEqual(len(result.value['data']), 2)
        self.assertEqual(result.value['count'], 2)

    def test_out_of_order_data(self):
        """
        测试乱序数据处理。
        """
        # 发送一个正常顺序的数据
        packet1 = self.create_packet(3000)
        self.operator.process(packet1)
        
        # 发送一个延迟但在容忍范围内的数据
        late_packet = self.create_packet(2000)
        self.operator.process(late_packet)
        
        # 验证两个数据都被接受
        window = self.operator._get_window(packet1.key, packet1.timestamp)
        self.assertEqual(len(window.data), 2)
        
        # 发送一个超出容忍范围的数据
        very_late_packet = self.create_packet(1000)
        very_late_packet.watermark = self.base_time  # 设置一个很老的水位线
        self.operator.process(very_late_packet)
        
        # 验证超时数据被丢弃
        self.assertEqual(len(window.data), 2)

    def test_multiple_keys(self):
        """
        测试多个key的窗口处理。
        """
        # 发送不同key的数据
        packet1 = self.create_packet(1000, key="key1")
        packet2 = self.create_packet(2000, key="key2")
        
        self.operator.process(packet1)
        self.operator.process(packet2)
        
        # 验证为每个key创建了独立的窗口
        window1 = self.operator._get_window("key1", packet1.timestamp)
        window2 = self.operator._get_window("key2", packet2.timestamp)
        
        self.assertIsNotNone(window1)
        self.assertIsNotNone(window2)
        self.assertEqual(len(window1.data), 1)
        self.assertEqual(len(window2.data), 1)

    def test_sliding_window(self):
        """
        测试滑动窗口。
        """
        # 创建滑动窗口算子（窗口大小5秒，滑动步长2秒）
        slide_operator = WindowOperator({
            'operator_id': 'test_sliding_window',
            'window_size_ms': 5000,
            'slide_size_ms': 2000
        })
        
        mock_downstream = Mock()
        slide_operator.add_downstream(mock_downstream)
        
        # 发送数据
        for i in range(6):
            packet = self.create_packet(i * 1000)
            slide_operator.process(packet)
        
        # 发送触发水位线
        trigger_packet = self.create_packet(7000)
        slide_operator.process(trigger_packet)
        
        # 验证产生了多个重叠窗口的结果
        self.assertTrue(mock_downstream.process.call_count > 1)

    def test_window_cleanup(self):
        """
        测试窗口清理。
        """
        # 发送一些数据
        for i in range(3):
            packet = self.create_packet(i * 1000)
            self.operator.process(packet)
        
        # 发送一个远未来的数据触发清理
        future_packet = self.create_packet(self.window_size_ms * 3)
        self.operator.process(future_packet)
        
        # 验证旧窗口被清理
        self.assertEqual(len(self.operator.windows.get("test_key", {})), 1)

    def tearDown(self):
        """
        测试后的清理工作。
        """
        self.operator.close()

if __name__ == '__main__':
    unittest.main() 