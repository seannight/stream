"""
Module: test_pipeline.py
Description: Pipeline类的单元测试。
"""

import unittest
from unittest.mock import Mock, patch
import time
from typing import Any, Dict
from core.pipeline import Pipeline
from core.data_packet import DataPacket
from core.operator import Operator
from operators.map_operator import MapOperator
from operators.sink_operator import ConsoleSinkOperator

class MockSourceOperator(Operator):
    """
    用于测试的模拟源算子。
    """
    def __init__(self, config: Dict):
        super().__init__(config)
        self.data = config.get('test_data', [])
        self.current_index = 0
        
    def process(self, _: Any) -> None:
        """模拟数据源产生数据"""
        if self.current_index < len(self.data):
            data = self.data[self.current_index]
            self.current_index += 1
            packet = DataPacket(
                timestamp=int(time.time() * 1000),
                watermark=int(time.time() * 1000) - 1000,
                key=str(data.get('key', '')),
                value=data
            )
            self.send_to_downstream(packet)

class TestPipeline(unittest.TestCase):
    """
    测试Pipeline类的基本功能。
    """
    
    def setUp(self):
        """
        测试前的准备工作。
        """
        self.pipeline = Pipeline({
            'name': 'test_pipeline',
            'max_workers': 2
        })
        
        # 准备测试数据
        self.test_data = [
            {'key': 'k1', 'value': 1},
            {'key': 'k2', 'value': 2},
            {'key': 'k3', 'value': 3}
        ]
        
        # 创建测试算子
        self.source = MockSourceOperator({
            'operator_id': 'test_source',
            'test_data': self.test_data
        })
        
        self.map_operator = MapOperator({
            'operator_id': 'test_map',
            'map_function': lambda x: {'output': x['value'] * 2}
        })
        
        self.sink = ConsoleSinkOperator({
            'operator_id': 'test_sink',
            'batch_size': 1
        })

    def test_add_operator(self):
        """
        测试添加算子。
        """
        self.pipeline.add_operator(self.source)
        self.pipeline.add_operator(self.map_operator)
        
        self.assertIn(self.source.operator_id, self.pipeline.operators)
        self.assertIn(self.map_operator.operator_id, self.pipeline.operators)
        self.assertIn(self.source, self.pipeline.sources)

    def test_connect_operators(self):
        """
        测试连接算子。
        """
        self.pipeline.add_operator(self.source)
        self.pipeline.add_operator(self.map_operator)
        self.pipeline.connect(self.source, self.map_operator)
        
        self.assertIn(self.map_operator, self.source.downstream_operators)

    def test_pipeline_execution(self):
        """
        测试Pipeline执行。
        """
        # 构建Pipeline
        (self.pipeline
            .add_operator(self.source)
            .add_operator(self.map_operator)
            .add_operator(self.sink)
            .connect(self.source, self.map_operator)
            .connect(self.map_operator, self.sink))
        
        # 使用Mock替换sink的write方法
        mock_write = Mock()
        self.sink.write = mock_write
        
        # 启动Pipeline并等待一段时间
        with self.pipeline:
            time.sleep(0.1)  # 给予足够时间处理数据
            
        # 验证结果
        self.assertEqual(mock_write.call_count, len(self.test_data))
        
        # 验证处理结果
        for call_args in mock_write.call_args_list:
            packets = call_args[0][0]  # 获取write方法的第一个参数
            self.assertEqual(len(packets), 1)
            packet = packets[0]
            self.assertEqual(packet.value['output'], 
                           self.test_data[packet.key[-1]]['value'] * 2)

    def test_pipeline_error_handling(self):
        """
        测试Pipeline错误处理。
        """
        # 创建一个会抛出异常的map算子
        def error_map(_):
            raise ValueError("测试错误")
            
        error_operator = MapOperator({
            'operator_id': 'error_map',
            'map_function': error_map
        })
        
        # 构建Pipeline
        (self.pipeline
            .add_operator(self.source)
            .add_operator(error_operator)
            .add_operator(self.sink)
            .connect(self.source, error_operator)
            .connect(error_operator, self.sink))
        
        # 验证Pipeline能够优雅处理错误
        with self.assertLogs(level='ERROR') as log:
            with self.pipeline:
                time.sleep(0.1)
            
            # 验证错误被记录
            self.assertTrue(any('测试错误' in record.message 
                              for record in log.records))

    def test_pipeline_stop(self):
        """
        测试Pipeline停止。
        """
        # 构建Pipeline
        (self.pipeline
            .add_operator(self.source)
            .add_operator(self.sink)
            .connect(self.source, self.sink))
        
        # 启动Pipeline
        self.pipeline.start()
        
        # 停止Pipeline
        self.pipeline.stop()
        
        # 验证状态
        self.assertTrue(self.pipeline._stop_flag.is_set())
        self.assertIsNone(self.pipeline.executor)

    def tearDown(self):
        """
        测试后的清理工作。
        """
        if self.pipeline:
            self.pipeline.stop()

if __name__ == '__main__':
    unittest.main() 