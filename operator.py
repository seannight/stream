"""
Module: operator.py
Description: 流计算系统的基础算子抽象类。
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import logging
from core.data_packet import DataPacket

logger = logging.getLogger(__name__)

class Operator(ABC):
    """
    算子基类，定义了算子的基本接口和通用功能。
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化算子。

        Args:
            config: 算子配置
        """
        # 如果没有提供operator_id，则从name字段获取，或者生成一个默认值
        if 'operator_id' not in config:
            if 'name' in config:
                # 使用name作为operator_id的备选项
                config['operator_id'] = config['name']
                logger.warning(f"配置中缺少operator_id，使用name作为替代: {config['name']}")
            else:
                # 生成一个基于类名和对象ID的唯一标识符
                config['operator_id'] = f"{self.__class__.__name__}_{id(self)}"
                logger.warning(f"配置中缺少operator_id和name，使用自动生成的ID: {config['operator_id']}")
                
        self.operator_id = config['operator_id']
        self.downstream_operators: List[Operator] = []
        logger.info(f"Initialized {self.__class__.__name__} with id {self.operator_id}")
    
    def add_downstream(self, operator: 'Operator') -> None:
        """
        添加下游算子。

        Args:
            operator: 下游算子实例
        """
        self.downstream_operators.append(operator)
        logger.debug(f"Added downstream operator {operator.operator_id}")
    
    def send_to_downstream(self, packet: DataPacket) -> None:
        """
        发送数据包到下游算子。

        Args:
            packet: 要发送的数据包
        """
        for operator in self.downstream_operators:
            try:
                operator.process(packet)
            except Exception as e:
                logger.error(f"Error sending to {operator.operator_id}: {e}")
    
    @abstractmethod
    def process(self, packet: DataPacket) -> Optional[DataPacket]:
        """
        处理数据包。

        Args:
            packet: 输入的数据包

        Returns:
            Optional[DataPacket]: 处理后的数据包，如果不需要输出则返回None
        """
        pass
    
    def initialize(self) -> None:
        """
        初始化算子资源，在开始处理数据前调用。
        """
        pass
    
    def close(self) -> None:
        """
        关闭算子，释放资源。
        """
        pass

    def log_error(self, message: str, exc_info: bool = True) -> None:
        """
        统一的错误日志记录方法。

        Args:
            message (str): 错误消息
            exc_info (bool): 是否包含异常堆栈信息
        """
        logger.error(
            f"[{self.operator_id}] {message}",
            exc_info=exc_info
        )

    def __repr__(self) -> str:
        """
        返回算子的字符串表示。

        Returns:
            str: 算子的字符串表示
        """
        return f"{self.__class__.__name__}(operator_id={self.operator_id})" 