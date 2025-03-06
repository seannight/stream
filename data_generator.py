"""
Module: data_generator.py
Description: Kafka测试数据生成器，生成文本数据用于单词计数示例。
"""

import json
import time
import random
import logging
from typing import List
from kafka import KafkaProducer
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.data_packet import DataPacket

logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 示例文本数据
sample_texts = [
    "流计算系统处理实时数据流",
    "大数据时代需要高效的数据处理框架",
    "Kafka是优秀的消息队列中间件",
    "流式计算使数据处理更加实时高效",
    "窗口操作是流计算的重要概念",
    "状态管理让流计算可以处理复杂场景",
    "数据流就像小溪一样源源不断",
    "算子是数据加工的车间工人",
    "Pipeline把不同的算子连接起来",
    "Sink算子负责输出处理结果"
]

def main():
    # 创建Kafka生产者
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        logger.info("成功连接到Kafka")
    except Exception as e:
        logger.error(f"连接Kafka失败: {e}")
        return

    # 生成并发送数据
    try:
        while True:
            # 随机选择一条文本
            message = random.choice(sample_texts)
            
            # 创建更适合处理的数据格式
            current_time = int(time.time() * 1000)
            
            # 方式1: 直接发送文本（现在我们的source_operator已经能处理这种格式）
            producer.send('word_count_input_new', value=message.encode('utf-8'))
            
            # 方式2: 也可以发送格式化的JSON数据
            # data_packet = DataPacket(
            #     timestamp=current_time,
            #     watermark=current_time,
            #     key="text",
            #     value=message
            # )
            # producer.send('word_count_input_new', value=data_packet.to_json().encode('utf-8'))
            
            logger.info(f"发送消息: {message}")
            producer.flush()
            
            # 暂停一会再发送下一条
            time.sleep(2)
    except KeyboardInterrupt:
        logger.info("数据生成器停止")
    except Exception as e:
        logger.error(f"发送数据时出错: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()