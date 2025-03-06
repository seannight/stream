"""
Module: source_operator.py
Description: 实现了流计算系统的源算子，包括Kafka源。
"""

from typing import Dict, Any, Optional
import json
import logging
import time
from kafka import KafkaConsumer
from core.operator import Operator
from core.data_packet import DataPacket

logger = logging.getLogger(__name__)

class KafkaSourceOperator(Operator):
    """Kafka源算子，从Kafka主题读取数据。"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.bootstrap_servers = config['bootstrap_servers']
        self.topic = config['topic']
        self.group_id = config.get('group_id', 'default_group')
        self.consumer_config = config.get('consumer_config', {})
        self.consumer = None
        
    def initialize(self) -> None:
        """初始化Kafka消费者"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                **self.consumer_config
            )
        except Exception as e:
            logger.error(f"Kafka连接失败: {e}")
            raise RuntimeError(f"Kafka连接失败: {e}")
            
    def process(self, _: Any) -> Optional[DataPacket]:
        """处理Kafka消息"""
        try:
            for message in self.consumer:
                packet = self._process_message(message)
                if packet:
                    self.send_to_downstream(packet)
        except Exception as e:
            logger.error(f"消息处理失败: {e}")
            
    def _process_message(self, message):
        """处理从Kafka接收到的消息"""
        try:
            # 尝试使用多种方式解析消息
            if message is None:
                logger.warning("收到空消息，跳过处理")
                return None
                
            # 获取消息值
            message_value = message.value
            
            # 根据消息类型进行处理
            if isinstance(message_value, (bytes, bytearray)):
                # 如果是字节类型，尝试解码为字符串
                message_str = message_value.decode('utf-8')
                
                # 尝试将字符串解析为JSON
                try:
                    # 尝试作为JSON解析
                    data = json.loads(message_str)
                    # 如果是字典格式，尝试创建DataPacket
                    if isinstance(data, dict):
                        return DataPacket.from_dict(data)
                    # 如果是纯文本，创建简单的DataPacket
                    else:
                        current_time = int(time.time() * 1000)
                        return DataPacket(
                            timestamp=current_time,
                            watermark=current_time,
                            key=str(message.key) if message.key else "default",
                            value=data
                        )
                except json.JSONDecodeError:
                    # 不是JSON，作为纯文本处理
                    current_time = int(time.time() * 1000)
                    return DataPacket(
                        timestamp=current_time,
                        watermark=current_time,
                        key=str(message.key) if message.key else "default",
                        value=message_str
                    )
            else:
                # 处理其他类型的消息
                current_time = int(time.time() * 1000)
                return DataPacket(
                    timestamp=current_time,
                    watermark=current_time,
                    key=str(message.key) if message.key else "default",
                    value=str(message_value)
                )
        except Exception as e:
            logger.error(f"消息处理失败: {str(e)}")
            # 增加详细错误信息以便调试
            logger.debug(f"消息内容: {message.value if hasattr(message, 'value') else message}", exc_info=True)
            return None
            
    def close(self) -> None:
        """关闭Kafka消费者"""
        if self.consumer:
            self.consumer.close() 