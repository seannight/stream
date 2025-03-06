"""
Module: test_state.py
Description: StateBackend类的单元测试。
"""

import unittest
import os
import tempfile
import shutil
from typing import Dict, Any
import json
from core.state import StateBackend, MemoryStateBackend, FileStateBackend

class TestMemoryStateBackend(unittest.TestCase):
    """
    测试内存状态后端的基本功能。
    """
    
    def setUp(self):
        """
        测试前的准备工作。
        """
        self.state_backend = MemoryStateBackend()
        self.test_data = {
            'counter': 42,
            'values': [1, 2, 3],
            'metadata': {'version': '1.0'}
        }
        
    def test_save_load_state(self):
        """
        测试状态的保存和加载。
        """
        # 保存状态
        self.state_backend.save_state(
            operator_id='test_op',
            state_key='test_key',
            state=self.test_data
        )
        
        # 加载状态
        loaded_state = self.state_backend.load_state(
            operator_id='test_op',
            state_key='test_key'
        )
        
        # 验证状态一致性
        self.assertEqual(loaded_state, self.test_data)
        
    def test_clear_state(self):
        """
        测试状态清理。
        """
        # 保存状态
        self.state_backend.save_state(
            operator_id='test_op',
            state_key='test_key',
            state=self.test_data
        )
        
        # 清理状态
        self.state_backend.clear_state(
            operator_id='test_op',
            state_key='test_key'
        )
        
        # 验证状态已被清理
        loaded_state = self.state_backend.load_state(
            operator_id='test_op',
            state_key='test_key'
        )
        self.assertIsNone(loaded_state)
        
    def test_multiple_operators(self):
        """
        测试多个算子的状态隔离。
        """
        # 为不同算子保存状态
        self.state_backend.save_state('op1', 'key1', {'value': 1})
        self.state_backend.save_state('op2', 'key1', {'value': 2})
        
        # 验证状态隔离
        state1 = self.state_backend.load_state('op1', 'key1')
        state2 = self.state_backend.load_state('op2', 'key1')
        
        self.assertEqual(state1['value'], 1)
        self.assertEqual(state2['value'], 2)

class TestFileStateBackend(unittest.TestCase):
    """
    测试文件状态后端的基本功能。
    """
    
    def setUp(self):
        """
        测试前的准备工作。
        """
        # 创建临时目录
        self.test_dir = tempfile.mkdtemp()
        self.state_backend = FileStateBackend({
            'base_path': self.test_dir
        })
        
        self.test_data = {
            'counter': 42,
            'values': [1, 2, 3],
            'metadata': {'version': '1.0'}
        }
        
    def test_save_load_state(self):
        """
        测试状态的保存和加载。
        """
        # 保存状态
        self.state_backend.save_state(
            operator_id='test_op',
            state_key='test_key',
            state=self.test_data
        )
        
        # 验证文件存在
        state_path = os.path.join(
            self.test_dir,
            'test_op',
            'test_key.json'
        )
        self.assertTrue(os.path.exists(state_path))
        
        # 加载状态
        loaded_state = self.state_backend.load_state(
            operator_id='test_op',
            state_key='test_key'
        )
        
        # 验证状态一致性
        self.assertEqual(loaded_state, self.test_data)
        
    def test_state_persistence(self):
        """
        测试状态持久化。
        """
        # 保存状态
        self.state_backend.save_state(
            operator_id='test_op',
            state_key='test_key',
            state=self.test_data
        )
        
        # 创建新的状态后端实例
        new_backend = FileStateBackend({
            'base_path': self.test_dir
        })
        
        # 加载状态
        loaded_state = new_backend.load_state(
            operator_id='test_op',
            state_key='test_key'
        )
        
        # 验证状态一致性
        self.assertEqual(loaded_state, self.test_data)
        
    def test_error_handling(self):
        """
        测试错误处理。
        """
        # 测试无效路径
        with self.assertRaises(ValueError):
            FileStateBackend({'base_path': ''})
        
        # 测试无权限路径
        if os.name != 'nt':  # 跳过Windows测试
            readonly_dir = os.path.join(self.test_dir, 'readonly')
            os.mkdir(readonly_dir)
            os.chmod(readonly_dir, 0o444)
            
            backend = FileStateBackend({'base_path': readonly_dir})
            with self.assertRaises(OSError):
                backend.save_state('test_op', 'test_key', self.test_data)
                
    def test_clear_state(self):
        """
        测试状态清理。
        """
        # 保存状态
        self.state_backend.save_state(
            operator_id='test_op',
            state_key='test_key',
            state=self.test_data
        )
        
        # 清理状态
        self.state_backend.clear_state(
            operator_id='test_op',
            state_key='test_key'
        )
        
        # 验证文件已被删除
        state_path = os.path.join(
            self.test_dir,
            'test_op',
            'test_key.json'
        )
        self.assertFalse(os.path.exists(state_path))
        
    def tearDown(self):
        """
        测试后的清理工作。
        """
        # 清理临时目录
        shutil.rmtree(self.test_dir)

if __name__ == '__main__':
    unittest.main() 