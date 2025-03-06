"""
Module: keyby_operator.py
Description: 实现了流式计算系统的分区算子，用于将数据流按照指定的key进行分区。
"""

from typing import Dict, Optional, Callable, Any, Union
import logging
import hashlib
from core.operator import Operator
from core.data_packet import DataPacket

logger = logging.getLogger(__name__)

class KeyByOperator(Operator):
    """
    将数据流按照指定的key选择器函数进行分区的算子。
    
    该算子接收一个key选择器函数，根据数据包的值生成分区key，
    并将具有相同key的数据包路由到相同的下游分区。
    
    Attributes:
        key_selector (Callable): key选择器函数，从数据包的值中提取key
        num_partitions (int): 分区数量
        partition_function (Callable): 分区函数，将key映射到分区号
    """

    def __init__(self, config: Dict):
        """
        初始化 KeyByOperator。

        Args:
            config (Dict): 配置字典，必须包含以下键：
                - operator_id: 算子唯一标识符
                - key_selector: key选择器函数或字段名
                可选键：
                - num_partitions: 分区数量 (默认: 16)
                - partition_function: 自定义分区函数

        Raises:
            ValueError: 如果缺少必需的配置参数或参数无效
        """
        super().__init__(config)
        
        if 'key_selector' not in config:
            error_msg = "配置中缺少必需的 key_selector 参数"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        self.key_selector = self._create_key_selector(config['key_selector'])
        self.num_partitions = config.get('num_partitions', 16)
        self.partition_function = config.get('partition_function', self._default_partition)
        
        logger.info(
            f"Initialized KeyByOperator with num_partitions={self.num_partitions}, "
            f"key_selector={self.key_selector.__name__ if hasattr(self.key_selector, '__name__') else str(self.key_selector)}"
        )

    def _create_key_selector(self, selector_config: Union[str, Callable]) -> Callable:
        """
        创建key选择器函数。

        Args:
            selector_config: 可以是字段名字符串或自定义函数

        Returns:
            Callable: key选择器函数

        Raises:
            ValueError: 如果selector_config无效
        """
        if isinstance(selector_config, str):
            # 如果是字符串，创建一个提取指定字段的函数
            field_name = selector_config
            def field_selector(value: Any) -> Any:
                if isinstance(value, dict):
                    return str(value.get(field_name, ''))
                return str(value)
            field_selector.__name__ = f"select_{field_name}"
            return field_selector
        elif callable(selector_config):
            return selector_config
        else:
            error_msg = f"无效的key选择器配置类型: {type(selector_config)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def _default_partition(self, key: str) -> int:
        """
        默认的分区函数，使用MD5哈希将key映射到分区号。

        Args:
            key (str): 分区key

        Returns:
            int: 分区号 (0 到 num_partitions-1)
        """
        hash_value = int(hashlib.md5(str(key).encode()).hexdigest(), 16)
        return hash_value % self.num_partitions

    def process(self, data_packet: DataPacket) -> Optional[DataPacket]:
        """
        处理输入的数据包，应用key选择器并生成新的数据包。

        Args:
            data_packet (DataPacket): 输入的数据包

        Returns:
            Optional[DataPacket]: 带有新key的数据包，如果处理失败则返回None

        Raises:
            TypeError: 如果输入不是DataPacket类型
        """
        if not isinstance(data_packet, DataPacket):
            error_msg = f"输入必须是DataPacket类型，而不是 {type(data_packet)}"
            logger.error(error_msg)
            raise TypeError(error_msg)

        try:
            # 使用key选择器生成新的key
            new_key = self.key_selector(data_packet.value)
            if new_key is None:
                logger.warning(f"Key selector returned None for value: {data_packet.value}")
                return None
                
            # 计算分区号
            partition = self.partition_function(new_key)
            
            # 创建新的数据包
            new_packet = DataPacket(
                timestamp=data_packet.timestamp,
                watermark=data_packet.watermark,
                key=str(new_key),  # 确保key是字符串类型
                value=data_packet.value
            )
            
            # 记录分区信息
            logger.debug(
                f"Partitioned data: key={new_key}, "
                f"partition={partition}, "
                f"original_value={data_packet.value}"
            )
            
            # 发送到下游算子
            self.send_to_downstream(new_packet)
            return new_packet
            
        except Exception as e:
            self.log_error(
                f"Key选择器执行失败: {e}, input_value={data_packet.value}",
                exc_info=True
            )
            return None

    def initialize(self) -> None:
        """
        初始化分区算子。验证配置的有效性。
        """
        super().initialize()
        try:
            # 测试key选择器是否可用 - 使用DataPacket而不是字典
            from core.data_packet import DataPacket
            import time
            
            current_time = int(time.time() * 1000)
            test_packet = DataPacket(
                timestamp=current_time,
                watermark=current_time,
                key="test_key",
                value={"test": "value"}
            )
            
            # 使用DataPacket进行测试
            test_key = self.key_selector(test_packet)
            test_partition = self.partition_function(test_key)
            
            if not (0 <= test_partition < self.num_partitions):
                raise ValueError(
                    f"分区函数返回的分区号 {test_partition} 超出有效范围 "
                    f"[0, {self.num_partitions-1}]"
                )
                
            logger.info("Key选择器和分区函数测试成功")
            
        except Exception as e:
            error_msg = f"KeyByOperator初始化测试失败: {e}"
            logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg) 