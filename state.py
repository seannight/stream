"""
Module: state.py
Description: 实现了流式计算系统的状态管理接口和基本实现。
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging
import json
import os
import threading

logger = logging.getLogger(__name__)

class StateBackend(ABC):
    """
    状态后端抽象基类，定义了状态存储和加载的接口。
    
    所有具体的状态后端实现都需要继承这个基类。
    
    Attributes:
        config (Dict): 状态后端的配置参数
    """

    def __init__(self, config: Dict):
        """
        初始化状态后端。

        Args:
            config (Dict): 配置参数
        """
        self.config = config
        logger.info(f"Initialized {self.__class__.__name__}")

    @abstractmethod
    def load_state(self, operator_id: str, state_key: str, default_value: Any = None) -> Any:
        """
        加载指定算子和键的状态值。

        Args:
            operator_id (str): 算子ID
            state_key (str): 状态键
            default_value (Any): 如果状态不存在时的默认值

        Returns:
            Any: 状态值或默认值
        """
        raise NotImplementedError("必须实现load_state方法")

    @abstractmethod
    def save_state(self, operator_id: str, state_key: str, state_value: Any) -> None:
        """
        保存状态值。

        Args:
            operator_id (str): 算子ID
            state_key (str): 状态键
            state_value (Any): 要保存的状态值
        """
        raise NotImplementedError("必须实现save_state方法")

    @abstractmethod
    def clear_state(self, operator_id: str, state_key: str) -> None:
        """
        清除指定的状态。

        Args:
            operator_id (str): 算子ID
            state_key (str): 状态键
        """
        raise NotImplementedError("必须实现clear_state方法")

class MemoryStateBackend(StateBackend):
    """
    内存状态后端实现，将状态存储在内存中。
    主要用于开发和测试环境。
    
    Attributes:
        states (Dict): 存储状态的内存字典
    """

    def __init__(self, config: Dict):
        """
        初始化内存状态后端。

        Args:
            config (Dict): 配置参数
        """
        super().__init__(config)
        self._states: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        logger.info("Initialized MemoryStateBackend")

    def _get_state_key(self, operator_id: str, state_key: str) -> str:
        """
        生成内部状态键。

        Args:
            operator_id (str): 算子ID
            state_key (str): 状态键

        Returns:
            str: 内部状态键
        """
        return f"{operator_id}:{state_key}"

    def load_state(self, operator_id: str, state_key: str, default_value: Any = None) -> Any:
        """
        从内存中加载状态值。

        Args:
            operator_id (str): 算子ID
            state_key (str): 状态键
            default_value (Any): 默认值

        Returns:
            Any: 状态值或默认值
        """
        with self._lock:
            operator_states = self._states.get(operator_id, {})
            return operator_states.get(state_key, default_value)

    def save_state(self, operator_id: str, state_key: str, state_value: Any) -> None:
        """
        将状态值保存到内存。

        Args:
            operator_id (str): 算子ID
            state_key (str): 状态键
            state_value (Any): 状态值
        """
        with self._lock:
            if operator_id not in self._states:
                self._states[operator_id] = {}
            self._states[operator_id][state_key] = state_value
            logger.debug(f"Saved state for {operator_id}:{state_key}")

    def clear_state(self, operator_id: str, state_key: str) -> None:
        """
        清除指定的状态。

        Args:
            operator_id (str): 算子ID
            state_key (str): 状态键
        """
        with self._lock:
            if operator_id in self._states and state_key in self._states[operator_id]:
                del self._states[operator_id][state_key]
                logger.debug(f"Cleared state for {operator_id}:{state_key}")

class FileStateBackend(StateBackend):
    """
    文件状态后端实现，将状态持久化到文件系统。
    用于简单的持久化场景，生产环境建议使用分布式存储。
    
    Attributes:
        base_path (str): 状态文件存储的基础路径
    """

    def __init__(self, config: Dict):
        """
        初始化文件状态后端。

        Args:
            config (Dict): 配置参数，必须包含：
                - base_path: 状态文件存储的基础路径
        """
        super().__init__(config)
        
        if 'base_path' not in config:
            raise ValueError("FileStateBackend requires 'base_path' in config")
            
        self.base_path = config['base_path']
        self._ensure_base_path()
        self._lock = threading.Lock()
        logger.info(f"Initialized FileStateBackend with base_path: {self.base_path}")

    def _ensure_base_path(self) -> None:
        """确保基础路径存在"""
        try:
            os.makedirs(self.base_path, exist_ok=True)
        except OSError as e:
            logger.error(f"创建状态目录失败: {e}")
            raise

    def _get_state_path(self, operator_id: str, state_key: str) -> str:
        """
        生成状态文件路径。

        Args:
            operator_id (str): 算子ID
            state_key (str): 状态键

        Returns:
            str: 状态文件完整路径
        """
        # 使用安全的文件名
        safe_operator_id = operator_id.replace('/', '_').replace('\\', '_')
        safe_state_key = state_key.replace('/', '_').replace('\\', '_')
        return os.path.join(self.base_path, f"{safe_operator_id}_{safe_state_key}.json")

    def load_state(self, operator_id: str, state_key: str, default_value: Any = None) -> Any:
        """
        从文件加载状态值。

        Args:
            operator_id (str): 算子ID
            state_key (str): 状态键
            default_value (Any): 默认值

        Returns:
            Any: 状态值或默认值
        """
        with self._lock:
            file_path = self._get_state_path(operator_id, state_key)
            try:
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
                return default_value
            except Exception as e:
                logger.error(f"Failed to load state from {file_path}: {e}", exc_info=True)
                return default_value

    def save_state(self, operator_id: str, state_key: str, state_value: Any) -> None:
        """
        将状态值保存到文件。

        Args:
            operator_id (str): 算子ID
            state_key (str): 状态键
            state_value (Any): 状态值
        """
        with self._lock:
            file_path = self._get_state_path(operator_id, state_key)
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(state_value, f, ensure_ascii=False, indent=2)
                logger.debug(f"Saved state to {file_path}")
            except Exception as e:
                logger.error(f"Failed to save state to {file_path}: {e}", exc_info=True)
                raise

    def clear_state(self, operator_id: str, state_key: str) -> None:
        """
        删除状态文件。

        Args:
            operator_id (str): 算子ID
            state_key (str): 状态键
        """
        with self._lock:
            file_path = self._get_state_path(operator_id, state_key)
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.debug(f"Cleared state file {file_path}")
            except Exception as e:
                logger.error(f"Failed to clear state file {file_path}: {e}", exc_info=True)
                raise