"""
Module: map_operator.py
Description: 实现了流式计算系统的映射算子，用于对数据流中的每个元素进行转换。
"""

from typing import Dict, Optional, Callable, Any
import logging
import time
from core.operator import Operator
from core.data_packet import DataPacket

logger = logging.getLogger(__name__)

class MapOperator(Operator):
    """
    对数据流中的每个元素应用映射函数进行转换的算子。
    
    该算子接收一个映射函数，将其应用于每个输入数据包的值，并生成新的数据包发送给下游。
    
    Attributes:
        map_function (Callable): 映射函数，接收一个输入值并返回转换后的值
        preserve_key (bool): 是否保留原始数据包的key (默认: True)
    """

    def __init__(self, config: Dict):
        """
        初始化 MapOperator。

        Args:
            config (Dict): 配置字典，必须包含以下键：
                - operator_id: 算子唯一标识符
                - map_function: 映射函数，可以是 callable 对象或其字符串表示
                可选键：
                - preserve_key: 是否保留原始数据包的key (默认: True)

        Raises:
            ValueError: 如果缺少必需的配置参数或map_function无效
        """
        super().__init__(config)
        
        if 'map_function' not in config:
            error_msg = "配置中缺少必需的 map_function 参数"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        self.map_function = self._load_map_function(config['map_function'])
        self.preserve_key = config.get('preserve_key', True)
        
        logger.info(
            f"Initialized MapOperator with preserve_key={self.preserve_key}, "
            f"map_function={self.map_function.__name__ if hasattr(self.map_function, '__name__') else str(self.map_function)}"
        )

    def _load_map_function(self, function_config: Any) -> Callable:
        """
        加载映射函数。支持直接传入callable对象或通过字符串表达式定义函数。

        Args:
            function_config: 映射函数配置，可以是callable对象或字符串

        Returns:
            Callable: 加载的映射函数

        Raises:
            ValueError: 如果函数配置无效或无法加载
        """
        if isinstance(function_config, str):
            try:
                # 警告：在生产环境中应该使用更安全的函数加载机制
                local_dict = {}
                exec(f"map_func = {function_config}", {}, local_dict)
                return local_dict['map_func']
            except Exception as e:
                error_msg = f"无法从字符串加载映射函数: {e}"
                logger.error(error_msg, exc_info=True)
                raise ValueError(error_msg)
        elif callable(function_config):
            return function_config
        else:
            error_msg = f"无效的映射函数配置类型: {type(function_config)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def process(self, data_packet: DataPacket) -> Optional[DataPacket]:
        """
        处理输入的数据包，应用映射函数并生成新的数据包。

        Args:
            data_packet (DataPacket): 输入的数据包

        Returns:
            Optional[DataPacket]: 转换后的新数据包，如果处理失败则返回None

        Raises:
            TypeError: 如果输入不是DataPacket类型
        """
        if not isinstance(data_packet, DataPacket):
            error_msg = f"输入必须是DataPacket类型，而不是 {type(data_packet)}"
            logger.error(error_msg)
            raise TypeError(error_msg)

        try:
            # 应用映射函数到数据包的值
            mapped_value = self.map_function(data_packet.value)
            
            # 创建新的数据包
            new_packet = DataPacket(
                timestamp=data_packet.timestamp,
                watermark=data_packet.watermark,
                key=data_packet.key if self.preserve_key else str(mapped_value),
                value=mapped_value
            )
            
            # 发送到下游算子
            self.send_to_downstream(new_packet)
            
            logger.debug(
                f"Mapped value from {data_packet.value} to {mapped_value}, "
                f"key={'preserved: ' + data_packet.key if self.preserve_key else 'new: ' + str(mapped_value)}"
            )
            
            return new_packet
            
        except Exception as e:
            self.log_error(
                f"映射函数执行失败: {e}, input_value={data_packet.value}",
                exc_info=True
            )
            return None

    def initialize(self) -> None:
        """
        初始化映射算子。可以在这里进行一些预处理工作。
        """
        super().initialize()
        try:
            # 测试映射函数是否可用 - 使用DataPacket进行测试而不是字典
            from core.data_packet import DataPacket
            current_time = int(time.time() * 1000)
            test_packet = DataPacket(
                timestamp=current_time,
                watermark=current_time,
                key="test_key",
                value={"test": "value"}
            )
            # 注意：不再直接传递字典，而是传递DataPacket对象
            self.map_function(test_packet)
            logger.info("映射函数测试成功")
        except Exception as e:
            error_msg = f"映射函数测试失败: {e}"
            logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg)

class SimpleMapOperator(MapOperator):
    """
    简单映射算子的实现，用于常见的值转换场景。
    提供了一些预定义的映射函数。
    """

    @staticmethod
    def identity(value: Any) -> Any:
        """
        身份映射函数，直接返回输入值。

        Args:
            value: 输入值

        Returns:
            输入值本身
        """
        return value

    @staticmethod
    def to_string(value: Any) -> str:
        """
        将输入值转换为字符串。

        Args:
            value: 输入值

        Returns:
            str: 输入值的字符串表示
        """
        return str(value)

    @staticmethod
    def extract_field(field_name: str) -> Callable:
        """
        创建一个从字典中提取指定字段的映射函数。

        Args:
            field_name (str): 要提取的字段名

        Returns:
            Callable: 提取字段的映射函数
        """
        def _extract(value: Dict) -> Any:
            return value.get(field_name)
        _extract.__name__ = f"extract_{field_name}"
        return _extract

    def __init__(self, config: Dict):
        """
        初始化简单映射算子。

        Args:
            config (Dict): 配置字典，可以使用预定义的映射函数名称：
                - map_function: 'identity' | 'to_string' | {'extract_field': 'field_name'}
        """
        # 处理预定义映射函数
        if isinstance(config.get('map_function'), str):
            if config['map_function'] == 'identity':
                config['map_function'] = self.identity
            elif config['map_function'] == 'to_string':
                config['map_function'] = self.to_string
        elif isinstance(config.get('map_function'), dict):
            if 'extract_field' in config['map_function']:
                field_name = config['map_function']['extract_field']
                config['map_function'] = self.extract_field(field_name)
        
        super().__init__(config) 