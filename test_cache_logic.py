#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试修改后的缓存逻辑
"""

import pandas as pd
import os
from datetime import datetime
from config import CONFIG

def test_cache_logic():
    print("测试缓存逻辑...")
    
    # 创建测试数据
    test_symbol = "000001"
    test_date = "2023-01-01"
    
    # 检查合并文件中是否包含该股票数据
    prices_path = os.path.join(CONFIG.data_dir, "prices.csv")
    if os.path.exists(prices_path):
        try:
            df = pd.read_csv(prices_path)
            symbol_df = df[df['symbol'] == test_symbol]
            if not symbol_df.empty:
                last_date = pd.to_datetime(symbol_df['date']).max()
                print(f"在合并文件中找到 {test_symbol} 的数据，最后日期: {last_date.strftime('%Y-%m-%d')}")
            else:
                print(f"在合并文件中未找到 {test_symbol} 的数据")
        except Exception as e:
            print(f"读取合并文件出错: {e}")
    else:
        print("未找到合并文件")

if __name__ == "__main__":
    test_cache_logic()
