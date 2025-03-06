"""
Module: word_count.py
Description: 使用流计算系统实现实时单词计数示例。
"""

import logging
import sys
import os
import time

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.pipeline import Pipeline
from operators.source_operator import KafkaSourceOperator
from operators.map_operator import MapOperator
from operators.keyby_operator import KeyByOperator
from operators.reduce_operator import ReduceOperator
from operators.sink_operator import ConsoleSinkOperator
from core.data_packet import DataPacket

# 配置日志
logging.basicConfig(level=logging.DEBUG, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def text_to_words(data_packet):
    """将文本分割成单词列表"""
    # 添加调试日志
    logger.info(f"收到数据包: {data_packet}")
    
    # 确保data_packet是DataPacket类型，或者兼容字典形式处理  
    if hasattr(data_packet, 'value'):
        value = data_packet.value
    elif isinstance(data_packet, dict) and 'value' in data_packet:
        value = data_packet['value']
    else:
        value = data_packet  # 直接使用传入值
        
    # 处理字符串类型的value
    if isinstance(value, str):
        words = value.split()
        # 为每个单词创建一个新的数据包
        result = []
        
        # 设置时间戳和水印（如果是DataPacket，则使用其值）
        timestamp = getattr(data_packet, 'timestamp', int(time.time() * 1000))
        watermark = getattr(data_packet, 'watermark', timestamp)
        
        for word in words:
            new_packet = DataPacket(
                timestamp=timestamp,
                watermark=watermark,
                key=word,
                value=1  # 每个单词初始计数为1
            )
            result.append(new_packet)
        return result
    else:
        logger.warning(f"收到非文本数据: {value}")
        return []

def select_word(data_packet):
    """使用单词作为key"""
    # 增加健壮性处理
    if hasattr(data_packet, 'key'):
        return data_packet.key
    elif isinstance(data_packet, dict) and 'key' in data_packet:
        return data_packet['key']
    else:
        # 如果都没有key，则返回一个默认值
        logger.warning(f"输入数据没有key属性: {data_packet}")
        return "unknown"

def count_reducer(accumulated, current):
    """单词计数的归约函数"""
    try:
        # 确保获取正确的值，无论是DataPacket还是普通值
        if hasattr(accumulated, 'value'):
            acc_value = accumulated.value 
        elif isinstance(accumulated, dict) and 'value' in accumulated:
            acc_value = accumulated['value']
        else:
            acc_value = accumulated
            
        if hasattr(current, 'value'):
            cur_value = current.value
        elif isinstance(current, dict) and 'value' in current:
            cur_value = current['value']
        else:
            cur_value = current
        
        # 类型转换，确保可以相加
        try:
            acc_value = int(acc_value) if not isinstance(acc_value, (int, float)) else acc_value
        except (ValueError, TypeError):
            logger.warning(f"无法将累积值转换为数值类型: {acc_value}，使用默认值1")
            acc_value = 1
            
        try:
            cur_value = int(cur_value) if not isinstance(cur_value, (int, float)) else cur_value
        except (ValueError, TypeError):
            logger.warning(f"无法将当前值转换为数值类型: {cur_value}，使用默认值1")
            cur_value = 1
            
        # 将当前值添加到累积值
        new_value = acc_value + cur_value
        
        # 获取其他必要的字段
        if hasattr(accumulated, 'timestamp') and hasattr(current, 'timestamp'):
            timestamp = max(accumulated.timestamp, current.timestamp)
        else:
            timestamp = int(time.time() * 1000)
            
        if hasattr(accumulated, 'watermark') and hasattr(current, 'watermark'):
            watermark = min(accumulated.watermark, current.watermark)
        else:
            watermark = timestamp
            
        if hasattr(accumulated, 'key'):
            key = accumulated.key
        elif hasattr(current, 'key'):
            key = current.key
        else:
            key = "default_key"
        
        # 创建新的数据包作为结果
        result = DataPacket(
            timestamp=timestamp,
            watermark=watermark,
            key=key,
            value=new_value
        )
        
        # 添加调试日志
        logger.debug(f"归约: {key} - 当前值: {cur_value}, 新总数: {new_value}")
        
        return result
    except Exception as e:
        logger.error(f"归约函数执行失败: {e}", exc_info=True)
        raise

def main():
    try:
        logger.info("启动单词计数示例程序...")
        
        # 创建pipeline
        logger.info("创建Pipeline...")
        pipeline = Pipeline({"name": "word_count"})
        
        # 从Kafka读取数据
        logger.info("配置KafkaSourceOperator...")
        source = KafkaSourceOperator({
            "name": "kafka_source",
            "operator_id": "kafka_source",
            "bootstrap_servers": "localhost:9092",
            "topic": "word_count_input_new",
            "group_id": "word_count_group_new"
        })
        
        # 将文本分割成单词
        logger.info("配置MapOperator...")
        map_op = MapOperator({
            "name": "split_text", 
            "operator_id": "split_text",
            "map_function": text_to_words
        })
        
        # 按单词分组
        logger.info("配置KeyByOperator...")
        keyby_op = KeyByOperator({
            "name": "group_by_word", 
            "operator_id": "group_by_word",
            "key_selector": select_word
        })
        
        # 计算每个单词的出现次数
        logger.info("配置ReduceOperator...")
        reduce_op = ReduceOperator({
            "name": "count_words", 
            "operator_id": "count_words",
            "reduce_function": count_reducer,
            "window_size_ms": 5000  # 5秒窗口
        })
        
        # 将结果输出到控制台
        logger.info("配置ConsoleSinkOperator...")
        sink = ConsoleSinkOperator({
            "name": "results_printer",
            "operator_id": "results_printer"
        })

        # 构建pipeline
        logger.info("添加所有算子到Pipeline...")
        pipeline.add_operator(source)
        pipeline.add_operator(map_op)
        pipeline.add_operator(keyby_op)
        pipeline.add_operator(reduce_op)
        pipeline.add_operator(sink)
        
        logger.info("连接Pipeline中的所有算子 构建DAG关系")
        pipeline.connect(source, map_op)
        pipeline.connect(map_op, keyby_op)
        pipeline.connect(keyby_op, reduce_op)
        pipeline.connect(reduce_op, sink)
        
        # 启动pipeline
        logger.info("正在启动Pipeline...")
        pipeline.start()
        
        logger.info("Pipeline running. Press Ctrl+C to stop.")
        
        # 保持主线程运行
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("收到用户中断信号，正在停止Pipeline...")
        if 'pipeline' in locals():
            pipeline.stop()
        logger.info("Pipeline已成功停止")
    except Exception as e:
        logger.error(f"单词计数示例发生错误: {e}", exc_info=True)
        if 'pipeline' in locals():
            try:
                logger.info("正在停止Pipeline...")
                pipeline.stop()
                logger.info("Pipeline已成功停止")
            except Exception as stop_error:
                logger.error(f"停止Pipeline时发生错误: {stop_error}")
        logger.error("请检查上述错误日志，解决问题后重新运行程序")
        sys.exit(1)  # 添加错误退出码

if __name__ == "__main__":
    try:
        logger.info("=== 单词计数应用程序启动 ===")
        main()
    except SystemExit as e:
        logger.error(f"程序异常退出，退出码: {e.code}")
    except Exception as e:
        logger.critical(f"未处理的异常: {e}", exc_info=True)
        sys.exit(2)
    finally:
        logger.info("=== 单词计数应用程序结束 ===")