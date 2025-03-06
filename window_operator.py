"""
Module: window_operator.py
Description: 实现了流式计算系统的窗口算子，用于处理基于时间的窗口计算。
"""

from typing import Dict, Optional, List, Any
import logging
import time
from dataclasses import dataclass
from core.operator import Operator
from core.data_packet import DataPacket
from core.state import StateBackend, MemoryStateBackend

logger = logging.getLogger(__name__)

@dataclass
class Window:
    """
    表示一个时间窗口。
    
    Attributes:
        start_time (int): 窗口开始时间（毫秒）
        end_time (int): 窗口结束时间（毫秒）
        data (List[DataPacket]): 窗口内的数据包列表
    """
    start_time: int
    end_time: int
    data: List[DataPacket]

class WindowOperator(Operator):
    """
    基于时间窗口的数据处理算子。
    
    支持滚动窗口(tumbling window)和滑动窗口(sliding window)，
    对窗口内的数据进行聚合计算。
    
    Attributes:
        window_size_ms (int): 窗口大小（毫秒）
        slide_size_ms (int): 滑动步长（毫秒），等于window_size_ms时为滚动窗口
        state_backend (StateBackend): 状态管理后端
        windows (Dict[str, Dict[int, Window]]): 按key分组的窗口数据
    """

    def __init__(self, config: Dict):
        """
        初始化 WindowOperator。

        Args:
            config (Dict): 配置字典，必须包含以下键：
                - window_size_ms: 窗口大小（毫秒）
                可选键：
                - slide_size_ms: 滑动步长（毫秒，默认等于window_size_ms）
                - state_backend: 状态后端配置
                - max_out_of_orderness: 最大允许的数据乱序时间（毫秒，默认: 5000）

        Raises:
            ValueError: 如果缺少必需的配置参数或参数无效
        """
        super().__init__(config)
        
        if 'window_size_ms' not in config:
            error_msg = "配置中缺少必需的 window_size_ms 参数"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        self.window_size_ms = config['window_size_ms']
        self.slide_size_ms = config.get('slide_size_ms', self.window_size_ms)
        self.max_out_of_orderness = config.get('max_out_of_orderness', 5000)
        
        if self.window_size_ms <= 0:
            raise ValueError("window_size_ms 必须大于0")
        if self.slide_size_ms <= 0:
            raise ValueError("slide_size_ms 必须大于0")
        if self.slide_size_ms > self.window_size_ms:
            raise ValueError("slide_size_ms 不能大于 window_size_ms")
            
        # 初始化状态后端
        state_config = config.get('state_backend', {'type': 'memory'})
        self.state_backend = self._create_state_backend(state_config)
        
        # 初始化窗口存储
        self.windows: Dict[str, Dict[int, Window]] = {}
        
        logger.info(
            f"Initialized WindowOperator with window_size={self.window_size_ms}ms, "
            f"slide_size={self.slide_size_ms}ms"
        )

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

    def _get_window_start_time(self, timestamp: int) -> int:
        """
        计算数据所属窗口的开始时间。

        Args:
            timestamp (int): 数据时间戳

        Returns:
            int: 窗口开始时间
        """
        return timestamp - (timestamp % self.slide_size_ms)

    def _create_window(self, start_time: int) -> Window:
        """
        创建新的窗口。

        Args:
            start_time (int): 窗口开始时间

        Returns:
            Window: 新创建的窗口
        """
        return Window(
            start_time=start_time,
            end_time=start_time + self.window_size_ms,
            data=[]
        )

    def _get_or_create_window(self, key: str, timestamp: int) -> Window:
        """
        获取或创建窗口。

        Args:
            key (str): 数据key
            timestamp (int): 数据时间戳

        Returns:
            Window: 对应的窗口对象
        """
        if key not in self.windows:
            self.windows[key] = {}
            
        window_start = self._get_window_start_time(timestamp)
        if window_start not in self.windows[key]:
            self.windows[key][window_start] = self._create_window(window_start)
            
        return self.windows[key][window_start]

    def _trigger_windows(self, watermark: int) -> None:
        """
        触发已完成的窗口计算。

        Args:
            watermark (int): 当前水位线
        """
        for key in list(self.windows.keys()):
            for start_time in list(self.windows[key].keys()):
                window = self.windows[key][start_time]
                if watermark >= window.end_time + self.max_out_of_orderness:
                    try:
                        self._process_window(key, window)
                        del self.windows[key][start_time]
                    except Exception as e:
                        self.log_error(f"处理窗口失败: {e}, key={key}, window={window}")

    def _process_window(self, key: str, window: Window) -> None:
        """
        处理单个窗口的数据。

        Args:
            key (str): 数据key
            window (Window): 要处理的窗口
        """
        if not window.data:
            return
            
        try:
            # 创建窗口结果数据包
            result_packet = DataPacket(
                timestamp=window.end_time,
                watermark=window.end_time,
                key=key,
                value={
                    'window_start': window.start_time,
                    'window_end': window.end_time,
                    'count': len(window.data),
                    'data': [dp.value for dp in window.data]
                }
            )
            
            # 发送到下游
            self.send_to_downstream(result_packet)
            
            logger.debug(
                f"Processed window: key={key}, "
                f"start={window.start_time}, "
                f"end={window.end_time}, "
                f"count={len(window.data)}"
            )
            
        except Exception as e:
            self.log_error(
                f"创建窗口结果失败: {e}, key={key}, "
                f"window_start={window.start_time}"
            )

    def process(self, data_packet: DataPacket) -> None:
        """
        处理输入的数据包，将其分配到对应的窗口。

        Args:
            data_packet (DataPacket): 输入的数据包

        Raises:
            TypeError: 如果输入不是DataPacket类型
        """
        if not isinstance(data_packet, DataPacket):
            error_msg = f"输入必须是DataPacket类型，而不是 {type(data_packet)}"
            logger.error(error_msg)
            raise TypeError(error_msg)

        try:
            # 获取对应的窗口
            window = self._get_or_create_window(
                data_packet.key,
                data_packet.timestamp
            )
            
            # 添加数据到窗口
            window.data.append(data_packet)
            
            # 触发已完成的窗口
            self._trigger_windows(data_packet.watermark)
            
        except Exception as e:
            self.log_error(
                f"窗口处理失败: {e}, key={data_packet.key}, "
                f"timestamp={data_packet.timestamp}"
            )

    def close(self) -> None:
        """
        关闭算子，处理所有未处理的窗口。
        """
        try:
            # 处理所有剩余窗口
            current_time = int(time.time() * 1000)
            self._trigger_windows(current_time)
            
            # 清理资源
            self.windows.clear()
            logger.info(f"Closing WindowOperator: {self.operator_id}")
            
        finally:
            super().close() 