#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主程序入口
负责处理命令行参数和启动程序
"""

import sys
import os
import logging
import argparse

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 从其他模块导入需要的功能
from data_fetch import (
    run_data_fetcher, 
    test_data_loading, 
    merge_batch_files,
    create_sample_data,
    strategy_params,
    DATA_DIR
)
from config import CONFIG

try:
    from stock_selection import run_stock_selection
except ImportError as e:
    print(f"警告: 无法导入策略模块: {e}")

# 尝试导入PyBroker回测模块
try:
    from pybroker_backtest import main as pybroker_backtest
    PYBROKER_AVAILABLE = True
except ImportError as e:
    print(f"警告: 无法导入PyBroker回测模块: {e}")
    PYBROKER_AVAILABLE = False
    pybroker_backtest = None

# 从菜单处理模块导入
from menu_handler import run_menu_loop

# 导入清理数据功能
from clean_price_cache import main as clean_price_cache

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, "main.log")),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="动态市现率策略系统")
    parser.add_argument('command', nargs='?', default='menu', choices=['fetch', 'backtest', 'pybroker', 'menu', 'clean'], help='执行命令: fetch(获取数据) 或 backtest(回测) 或 pybroker(PyBroker回测) 或 menu(菜单模式) 或 clean(清理数据)')
    parser.add_argument('--start-date', default=CONFIG.start_date, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', default=CONFIG.end_date, help='结束日期 (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    if args.command == 'fetch':
        run_data_fetcher()
    elif args.command == 'backtest':
        if PYBROKER_AVAILABLE and pybroker_backtest is not None:
            pybroker_backtest()
        else:
            print("PyBroker回测模块不可用")
    elif args.command == 'pybroker':
        if PYBROKER_AVAILABLE and pybroker_backtest is not None:
            pybroker_backtest()
        else:
            print("PyBroker回测模块不可用")
    elif args.command == 'menu':
        run_menu_loop()
    elif args.command == 'clean':
        clean_price_cache()

if __name__ == "__main__":
    main()