"""
Module: pipeline.py
Description: 实现了流式计算系统的Pipeline，负责组织和管理数据流的执行。
"""

from typing import Dict, List, Optional, Type, Any
import logging
import time
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from core.operator import Operator
from core.data_packet import DataPacket
from operators.source_operator import KafkaSourceOperator
from operators.sink_operator import BaseSinkOperator

logger = logging.getLogger(__name__)

class Pipeline:
    """
    流计算Pipeline，负责组织和管理数据流的执行。
    
    Pipeline将多个算子连接成一个有向无环图(DAG)，
    并负责调度和执行这些算子。
    
    Attributes:
        name (str): Pipeline名称
        operators (Dict[str, Operator]): 算子字典
        sources (List[Operator]): 源算子列表
        sinks (List[BaseSinkOperator]): 目标算子列表
        max_workers (int): 最大工作线程数
        executor (ThreadPoolExecutor): 线程池执行器
    """

    def __init__(self, config: Dict):
        """
        初始化Pipeline。

        Args:
            config (Dict): 配置字典，必须包含：
                - name: Pipeline名称
                可选键：
                - max_workers: 最大工作线程数 (默认: CPU核心数)

        Raises:
            ValueError: 如果配置参数无效
        """
        if 'name' not in config:
            raise ValueError("配置中缺少必需的 name 参数")
            
        self.name = config['name']
        self.max_workers = config.get('max_workers', None)  # None表示使用默认值
        
        self.operators: Dict[str, Operator] = {}
        self.sources: List[Operator] = []
        self.sinks: List[BaseSinkOperator] = []
        self.executor: Optional[ThreadPoolExecutor] = None
        
        # 用于停止Pipeline的标志
        self._stop_flag = threading.Event()
        
        logger.info(f"Initialized Pipeline: {self.name}")

    def add_operator(self, operator: Any) -> 'Pipeline':
        """
        Add an operator to the pipeline.
        
        Args:
            operator: The operator to add
            
        Returns:
            self: For method chaining
        """
        self.operators[operator.operator_id] = operator
        
        # 识别源算子和目标算子
        if isinstance(operator, KafkaSourceOperator):
            self.sources.append(operator)
        elif isinstance(operator, BaseSinkOperator):
            self.sinks.append(operator)
            
        logger.info(f"Added operator: {operator.operator_id}")
        return self

    def connect(self, from_operator: Operator, to_operator: Operator) -> 'Pipeline':
        """
        连接两个算子。

        Args:
            from_operator (Operator): 上游算子
            to_operator (Operator): 下游算子

        Returns:
            Pipeline: Pipeline实例，支持链式调用

        Raises:
            ValueError: 如果算子未添加到Pipeline
        """
        if from_operator.operator_id not in self.operators:
            raise ValueError(f"上游算子未添加到Pipeline: {from_operator.operator_id}")
        if to_operator.operator_id not in self.operators:
            raise ValueError(f"下游算子未添加到Pipeline: {to_operator.operator_id}")
            
        from_operator.add_downstream(to_operator)
        logger.info(f"Connected: {from_operator.operator_id} -> {to_operator.operator_id}")
        return self

    def _initialize_operators(self) -> None:
        """
        初始化所有算子。
        """
        failed_operators = []
        
        for operator in self.operators.values():
            try:
                operator.initialize()
            except Exception as e:
                error_msg = f"初始化算子失败: {operator.operator_id}, error: {e}"
                logger.error(error_msg)
                failed_operators.append((operator.operator_id, str(e)))
        
        # 如果有算子初始化失败，统一报告
        if failed_operators:
            error_details = "\n".join([f"- {op_id}: {err}" for op_id, err in failed_operators])
            raise RuntimeError(f"以下算子初始化失败:\n{error_details}")

    def _close_operators(self) -> None:
        """
        关闭所有算子。
        """
        for operator in reversed(list(self.operators.values())):
            try:
                operator.close()
            except Exception as e:
                logger.error(f"关闭算子失败: {operator.operator_id}, error: {e}")

    def start(self) -> None:
        """
        启动Pipeline执行。

        Raises:
            RuntimeError: 如果Pipeline配置无效或启动失败
        """
        if not self.sources:
            raise RuntimeError("Pipeline没有源算子")
        if not self.sinks:
            raise RuntimeError("Pipeline没有目标算子")
            
        try:
            # 初始化线程池
            self.executor = ThreadPoolExecutor(
                max_workers=self.max_workers,
                thread_name_prefix=f"Pipeline-{self.name}"
            )
            
            # 打印详细信息
            total_operators = len(self.operators)
            logger.info(f"Pipeline配置: {total_operators}个算子")
            for i, (op_id, op) in enumerate(self.operators.items(), 1):
                logger.info(f"  [{i}/{total_operators}] {op.__class__.__name__}: {op_id}")
            
            # 初始化所有算子
            try:
                logger.info("开始初始化所有算子...")
                self._initialize_operators()
                logger.info("所有算子初始化成功")
            except Exception as e:
                # 如果初始化失败，确保记录详细信息并清理资源
                logger.error(f"初始化算子失败: {e}")
                self.stop()  # 确保停止并清理资源
                raise  # 重新抛出异常
            
            # 清除停止标志
            self._stop_flag.clear()
            
            # 启动源算子
            logger.info(f"启动 {len(self.sources)} 个源算子...")
            for source in self.sources:
                self.executor.submit(self._run_source, source)
                logger.info(f"  源算子已启动: {source.operator_id}")
                
            logger.info(f"Started Pipeline: {self.name}")
            
        except Exception as e:
            self.stop()
            error_msg = f"启动Pipeline失败: {e}"
            logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg)

    def _run_source(self, source: Operator) -> None:
        """
        运行源算子。

        Args:
            source (Operator): 源算子
        """
        try:
            while not self._stop_flag.is_set():
                source.process(None)  # 源算子不需要输入
                
        except Exception as e:
            logger.error(f"源算子执行失败: {source.operator_id}, error: {e}", exc_info=True)
            self.stop()

    def stop(self) -> None:
        """
        停止Pipeline执行。
        """
        try:
            # 设置停止标志
            self._stop_flag.set()
            
            # 关闭线程池
            if self.executor:
                self.executor.shutdown(wait=True)
                self.executor = None
            
            # 关闭所有算子
            self._close_operators()
            
            logger.info(f"Stopped Pipeline: {self.name}")
            
        except Exception as e:
            logger.error(f"停止Pipeline失败: {e}", exc_info=True)
            raise

    def wait(self) -> None:
        """
        等待Pipeline执行完成。
        """
        try:
            while not self._stop_flag.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("收到停止信号")
            self.stop()

    def __enter__(self) -> 'Pipeline':
        """
        支持使用with语句。
        """
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        退出with语句时自动停止Pipeline。
        """
        self.stop()

    def run(self) -> None:
        """
        Execute the pipeline.
        """
        # TODO: Implement pipeline execution logic
        pass 