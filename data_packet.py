"""
Module: data_packet.py
Description: 定义了流式计算系统中的基础数据包类，用于在算子之间传递数据。
"""

from typing import Any, Dict
import json
import logging

logger = logging.getLogger(__name__)

class DataPacket:
    """
    表示流式计算系统中的数据包。
    
    数据包负责在不同算子之间传递数据，包含时间戳、水位线、分区键和实际数据。
    
    Attributes:
        timestamp (int): 数据包的事件时间戳（自epoch以来的毫秒数）
        watermark (int): 数据流的当前水位线（自epoch以来的毫秒数）
        key (str): 用于数据分区和状态管理的分区键
        value (Any): 数据负载，可以是任意Python对象
    """

    def __init__(self, timestamp: int, watermark: int, key: str, value: Any):
        """
        初始化一个数据包对象。

        Args:
            timestamp (int): 事件时间戳
            watermark (int): 水位线
            key (str): 分区键
            value (Any): 数据负载

        Raises:
            TypeError: 如果参数类型不正确
            ValueError: 如果timestamp或watermark为负数
        """
        # 参数类型检查
        if not isinstance(timestamp, int):
            raise TypeError("timestamp必须是整数类型")
        if not isinstance(watermark, int):
            raise TypeError("watermark必须是整数类型")
        if not isinstance(key, str):
            raise TypeError("key必须是字符串类型")
            
        # 参数值检查
        if timestamp < 0:
            raise ValueError("timestamp不能为负数")
        if watermark < 0:
            raise ValueError("watermark不能为负数")
            
        self.timestamp = timestamp
        self.watermark = watermark
        self.key = key
        self.value = value
        
        logger.debug(f"Created DataPacket: timestamp={timestamp}, key={key}")

    def to_dict(self) -> Dict:
        """
        将数据包对象转换为字典格式。

        Returns:
            Dict: 包含数据包所有属性的字典

        Example:
            >>> packet = DataPacket(1000, 900, "user_1", {"count": 1})
            >>> packet.to_dict()
            {'timestamp': 1000, 'watermark': 900, 'key': 'user_1', 'value': {'count': 1}}
        """
        try:
            return {
                "timestamp": self.timestamp,
                "watermark": self.watermark,
                "key": self.key,
                "value": self.value
            }
        except Exception as e:
            logger.error(f"Failed to convert DataPacket to dict: {e}", exc_info=True)
            raise

    @classmethod
    def from_dict(cls, data_dict: Dict) -> 'DataPacket':
        """
        从字典创建数据包对象。

        Args:
            data_dict (Dict): 包含数据包属性的字典

        Returns:
            DataPacket: 新创建的数据包对象

        Raises:
            ValueError: 如果字典缺少必需的键
            TypeError: 如果字典中的值类型不正确

        Example:
            >>> data = {'timestamp': 1000, 'watermark': 900, 'key': 'user_1', 'value': {'count': 1}}
            >>> packet = DataPacket.from_dict(data)
        """
        required_keys = ["timestamp", "watermark", "key", "value"]
        
        # 检查必需的键是否存在
        missing_keys = [key for key in required_keys if key not in data_dict]
        if missing_keys:
            error_msg = f"字典缺少必需的键: {missing_keys}"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        try:
            return cls(
                timestamp=data_dict["timestamp"],
                watermark=data_dict["watermark"],
                key=data_dict["key"],
                value=data_dict["value"]
            )
        except Exception as e:
            logger.error(f"Failed to create DataPacket from dict: {e}", exc_info=True)
            raise

    def to_json(self) -> str:
        """
        将数据包对象转换为JSON字符串。

        Returns:
            str: JSON格式的字符串表示

        Raises:
            TypeError: 如果对象无法序列化为JSON
        """
        try:
            return json.dumps(self.to_dict())
        except Exception as e:
            logger.error(f"Failed to convert DataPacket to JSON: {e}", exc_info=True)
            raise

    @classmethod
    def from_json(cls, json_str: str) -> 'DataPacket':
        """
        从JSON字符串创建数据包对象。

        Args:
            json_str (str): JSON格式的字符串

        Returns:
            DataPacket: 新创建的数据包对象

        Raises:
            json.JSONDecodeError: 如果JSON字符串格式不正确
        """
        try:
            data_dict = json.loads(json_str)
            return cls.from_dict(data_dict)
        except Exception as e:
            logger.error(f"Failed to create DataPacket from JSON: {e}", exc_info=True)
            raise

    def __eq__(self, other: object) -> bool:
        """
        比较两个数据包对象是否相等。

        Args:
            other (object): 要比较的另一个对象

        Returns:
            bool: 如果两个对象的所有属性都相等则返回True
        """
        if not isinstance(other, DataPacket):
            return False
        return (self.timestamp == other.timestamp and
                self.watermark == other.watermark and
                self.key == other.key and
                self.value == other.value)

    def __repr__(self) -> str:
        """
        返回数据包对象的字符串表示。

        Returns:
            str: 对象的字符串表示
        """
        return (f"DataPacket(timestamp={self.timestamp}, "
                f"watermark={self.watermark}, "
                f"key='{self.key}', "
                f"value={self.value})") 