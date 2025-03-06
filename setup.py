"""
Module: setup.py
Description: 安装流计算系统所需的依赖
"""

import subprocess
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dependencies = [
    "kafka-python",
    "pytest",
   
    # 其他依赖...
]

def install_dependencies():
    """
    安装项目所需的依赖
    
    @returns (bool): 安装是否成功
    """
    logger.info("开始安装依赖...")
    
    for dep in dependencies:
        try:
            logger.info(f"安装 {dep}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", dep])
        except subprocess.CalledProcessError as e:
            logger.error(f"安装 {dep} 失败: {e}")
            return False
    
    logger.info("所有依赖安装完成")
    return True

if __name__ == "__main__":
    success = install_dependencies()
    sys.exit(0 if success else 1) 