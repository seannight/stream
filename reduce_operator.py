"""
Module: reduce_operator.py
Description: 实现了流式计算系统的聚合算子，用于对分组数据进行聚合计算。
"""

from typing import Dict, Optional, Callable, Any
import logging
import time  # 添加time模块导入
from core.operator import Operator
from core.data_packet import DataPacket
from core.state import StateBackend, MemoryStateBackend

logger = logging.getLogger(__name__)

class ReduceOperator(Operator):
    """
    对分组数据进行聚合计算的算子。
    
    该算子接收一个归约函数，将其应用于同一个key的数据序列，
    维护每个key的聚合状态并输出更新后的结果。
    
    Attributes:
        reduce_function (Callable): 归约函数，接收两个值并返回聚合结果
        state_backend (StateBackend): 状态管理后端
        state_prefix (str): 状态键前缀
    """

    def __init__(self, config: Dict):
        """
        初始化 ReduceOperator。

        Args:
            config (Dict): 配置字典，必须包含以下键：
                - operator_id: 算子唯一标识符
                - reduce_function: 归约函数，可以是 callable 对象或其字符串表示
                可选键：
                - state_backend: 状态后端配置
                - state_prefix: 状态键前缀 (默认: "reduce_state")
                - window_size_ms: 窗口大小（毫秒）

        Raises:
            ValueError: 如果缺少必需的配置参数或参数无效
        """
        super().__init__(config)
        
        if 'reduce_function' not in config:
            error_msg = "配置中缺少必需的 reduce_function 参数"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        self.reduce_function = self._load_reduce_function(config['reduce_function'])
        self.state_prefix = config.get('state_prefix', 'reduce_state')
        
        # 初始化状态后端
        state_config = config.get('state_backend', {'type': 'memory'})
        self.state_backend = self._create_state_backend(state_config)
        
        logger.info(
            f"Initialized ReduceOperator with state_prefix={self.state_prefix}, "
            f"reduce_function={self.reduce_function.__name__ if hasattr(self.reduce_function, '__name__') else str(self.reduce_function)}"
        )

    def _load_reduce_function(self, function_config: Any) -> Callable:
        """
        加载归约函数。支持直接传入callable对象或通过字符串表达式定义函数。

        Args:
            function_config: 归约函数配置，可以是callable对象或字符串

        Returns:
            Callable: 加载的归约函数

        Raises:
            ValueError: 如果函数配置无效或无法加载
        """
        if isinstance(function_config, str):
            try:
                # 警告：在生产环境中应该使用更安全的函数加载机制
                local_dict = {}
                exec(f"reduce_func = {function_config}", {}, local_dict)
                return local_dict['reduce_func']
            except Exception as e:
                error_msg = f"无法从字符串加载归约函数: {e}"
                logger.error(error_msg, exc_info=True)
                raise ValueError(error_msg)
        elif callable(function_config):
            return function_config
        else:
            error_msg = f"无效的归约函数配置类型: {type(function_config)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def _create_state_backend(self, state_config: Dict) -> StateBackend:
        """
        创建状态后端实例。

        Args:
            state_config (Dict): 状态后端配置

        Returns:
            StateBackend: 状态后端实例
        """
        backend_type = state_config.get('type', 'memory')
        if backend_type == 'memory':
            return MemoryStateBackend(state_config)
        else:
            error_msg = f"不支持的状态后端类型: {backend_type}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def _get_state_key(self, key: str) -> str:
        """
        生成状态键。

        Args:
            key (str): 数据的key

        Returns:
            str: 完整的状态键
        """
        return f"{self.state_prefix}:{key}"

    def process(self, data_packet: DataPacket) -> Optional[DataPacket]:
        """
        处理输入的数据包，执行聚合计算并更新状态。

        Args:
            data_packet (DataPacket): 输入的数据包

        Returns:
            Optional[DataPacket]: 聚合后的数据包，如果处理失败则返回None

        Raises:
            TypeError: 如果输入不是DataPacket类型
        """
        if not isinstance(data_packet, DataPacket):
            error_msg = f"输入必须是DataPacket类型，而不是 {type(data_packet)}"
            logger.error(error_msg)
            raise TypeError(error_msg)

        try:
            # 获取当前key的状态
            state_key = self._get_state_key(data_packet.key)
            current_state = self.state_backend.load_state(
                self.operator_id,
                state_key
            )
            
            # 计算新的聚合值
            if current_state is None:
                new_value = data_packet.value
            else:
                new_value = self.reduce_function(current_state, data_packet.value)
            
            # 更新状态
            self.state_backend.save_state(
                self.operator_id,
                state_key,
                new_value
            )
            
            # 创建输出数据包
            result_packet = DataPacket(
                timestamp=data_packet.timestamp,
                watermark=data_packet.watermark,
                key=data_packet.key,
                value=new_value
            )
            
            # 发送到下游
            self.send_to_downstream(result_packet)
            return result_packet
            
        except Exception as e:
            self.log_error(
                f"聚合计算失败: {e}, key={data_packet.key}, value={data_packet.value}",
                exc_info=True
            )
            return None

    def initialize(self) -> None:
        """
        初始化聚合算子。验证配置的有效性。
        """
        super().initialize()
        try:
            # 测试归约函数是否可用 - 使用DataPacket对象并且使用整数值进行测试
            from core.data_packet import DataPacket
            import time
            
            current_time = int(time.time() * 1000)
            # 创建两个测试数据包，使用整数值而不是字典
            test_packet1 = DataPacket(
                timestamp=current_time,
                watermark=current_time,
                key="test_key",
                value=1  # 使用整数值，而不是字典
            )
            
            test_packet2 = DataPacket(
                timestamp=current_time,
                watermark=current_time,
                key="test_key",
                value=2  # 使用整数值，而不是字典
            )
            
            # 使用DataPacket测试归约函数
            test_result = self.reduce_function(test_packet1, test_packet2)
            
            if test_result is None:
                raise ValueError("归约函数返回了None")
                
            logger.info("归约函数测试成功")
            
        except Exception as e:
            error_msg = f"ReduceOperator初始化测试失败: {e}"
            logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg)

    def close(self) -> None:
        """
        关闭算子，清理资源。
        """
        try:
            # 这里可以添加状态持久化等清理工作
            logger.info(f"Closing ReduceOperator: {self.operator_id}")
        finally:
            super().close() 