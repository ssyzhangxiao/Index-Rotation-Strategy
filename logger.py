import logging
import sys
import os

def setup_logger(log_file_path):
    """配置日志记录器"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # 清除已有处理器避免重复日志
    if logger.hasHandlers():
        logger.handlers.clear()

    # 添加文件处理器
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 添加控制台输出处理器
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
