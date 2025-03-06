"""
Module: sink_operator.py
Description: 实现了流计算系统的输出算子，包括Kafka输出和控制台输出。
"""

from typing import Dict, Any, List, Optional
import json
import logging
from kafka import KafkaProducer
from core.operator import Operator
from core.data_packet import DataPacket

logger = logging.getLogger(__name__)

class BaseSinkOperator(Operator):
    """
    Sink算子的抽象基类。
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.batch_size = config.get('batch_size', 1)
        self.batch_buffer: List[DataPacket] = []
    
    def process(self, packet: DataPacket) -> None:
        """
        处理数据包。
        
        Args:
            packet: 输入的数据包
        """
        self.batch_buffer.append(packet)
        if len(self.batch_buffer) >= self.batch_size:
            self.write(self.batch_buffer)
            self.batch_buffer.clear()
    
    def write(self, packets: List[DataPacket]) -> None:
        """
        写入数据包到目标系统。
        
        Args:
            packets: 要写入的数据包列表
        """
        raise NotImplementedError("Sink算子必须实现write方法")
    
    def close(self) -> None:
        """关闭资源并刷新剩余数据"""
        if self.batch_buffer:
            self.write(self.batch_buffer)
            self.batch_buffer.clear()
        super().close()

class KafkaSinkOperator(BaseSinkOperator):
    """
    Kafka输出算子，将数据写入Kafka主题。
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        if 'bootstrap_servers' not in config:
            raise ValueError("必须提供bootstrap_servers")
        if 'topic' not in config or not config['topic']:
            raise ValueError("必须提供有效的topic")
            
        self.bootstrap_servers = config['bootstrap_servers']
        self.topic = config['topic']
        self.producer_config = config.get('producer_config', {})
        self.producer = None
    
    def initialize(self) -> None:
        """初始化Kafka生产者"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                **self.producer_config
            )
        except Exception as e:
            logger.error(f"Kafka连接失败: {e}")
            raise RuntimeError(f"Kafka连接失败: {e}")
    
    def write(self, packets: List[DataPacket]) -> None:
        """
        将数据包写入Kafka。
        
        Args:
            packets: 要写入的数据包列表
        """
        try:
            for packet in packets:
                future = self.producer.send(
                    self.topic,
                    value=packet.value,
                    key=packet.key,
                    timestamp_ms=packet.timestamp
                )
                # 等待发送完成
                future.get(timeout=10)
        except Exception as e:
            logger.error(f"发送消息到Kafka失败: {e}")
            raise
    
    def close(self) -> None:
        """关闭Kafka生产者"""
        try:
            super().close()
        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
                logger.info("Kafka producer closed")

class ConsoleSinkOperator(BaseSinkOperator):
    """将数据输出到控制台的Sink算子"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化控制台输出Sink算子
        
        Args:
            config: 算子配置，必须包含以下键：
                - operator_id: 算子唯一标识符
                可选键：
                - batch_size: 批处理大小
        """
        super().__init__(config)
        self.result_counter = 0
    
    def process(self, packet: DataPacket) -> None:
        """
        将数据包内容打印到控制台
        
        Args:
            packet: 要处理的数据包
        """
        self.result_counter += 1
        
        # 美化输出格式
        if isinstance(packet.value, (int, float)):
            # 对于数值结果（如单词计数），使用表格样式输出
            print("\n" + "="*50)
            print(f"🔢 结果 #{self.result_counter} | ⏰ 时间戳: {packet.timestamp}")
            print("-"*50)
            print(f"📊 单词: '{packet.key}' | 出现次数: {packet.value}")
            print("="*50 + "\n")
        else:
            # 对于其他类型的结果，使用一般格式
            print("\n" + "="*50)
            print(f"📦 数据包 #{self.result_counter} | ⏰ 时间戳: {packet.timestamp}")
            print("-"*50)
            print(f"🔑 键: {packet.key}")
            print(f"📋 值: {packet.value}")
            print("="*50 + "\n")
        
        # 记录日志
        logger.info(f"输出结果: key={packet.key}, value={packet.value}")

    def close(self) -> None:
        """关闭资源并刷新剩余数据"""
        if self.batch_buffer:
            self.write(self.batch_buffer)
            self.batch_buffer.clear()
        super().close()

class FileSinkOperator(BaseSinkOperator):
    def __init__(self, config: Dict[str, Any]):
        """
        初始化文件输出Sink算子
        
        Args:
            config: 算子配置，必须包含以下键：
                - operator_id: 算子唯一标识符
                - file_path: 输出文件路径
        """
        super().__init__(config)
        self.file_path = config['file_path']
        self.file = None
        
    def initialize(self) -> None:
        """打开文件准备写入"""
        try:
            self.file = open(self.file_path, 'w')
            logger.info(f"打开文件 {self.file_path} 用于写入")
        except Exception as e:
            logger.error(f"打开文件失败: {e}")
            raise RuntimeError(f"打开文件失败: {e}")
        
    def process(self, data_packet: DataPacket) -> None:
        """将数据包写入文件"""
        if not self.file:
            self.initialize()
        self.file.write(f"{data_packet.key}: {data_packet.value}\n")
        self.file.flush()
        
    def close(self) -> None:
        """关闭文件"""
        if self.file:
            self.file.close()
            logger.info(f"关闭文件 {self.file_path}")
            self.file = None 