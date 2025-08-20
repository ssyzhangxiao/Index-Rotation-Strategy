#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
诊断数据不一致问题
"""

import pandas as pd
import os
from config import CONFIG

def diagnose_data_issue():
    print("诊断数据不一致问题...")
    
    # 读取constituents.csv
    constituents_path = os.path.join(CONFIG.data_dir, "constituents.csv")
    if os.path.exists(constituents_path):
        constituents_df = pd.read_csv(constituents_path, dtype={'symbol': str})
        constituents_symbols = set(constituents_df['symbol'].unique())
        print(f"constituents.csv 中有 {len(constituents_symbols)} 只股票")
    else:
        print("未找到 constituents.csv 文件")
        return
    
    # 读取prices.csv
    prices_path = os.path.join(CONFIG.data_dir, "prices.csv")
    if os.path.exists(prices_path):
        prices_df = pd.read_csv(prices_path, dtype={'symbol': str})
        prices_symbols = set(prices_df['symbol'].unique())
        print(f"prices.csv 中有 {len(prices_symbols)} 只股票/指数")
    else:
        print("未找到 prices.csv 文件")
        return
    
    # 比较差异
    missing_in_prices = constituents_symbols - prices_symbols
    extra_in_prices = prices_symbols - constituents_symbols
    
    print(f"在constituents.csv中但不在prices.csv中的股票数量: {len(missing_in_prices)}")
    if missing_in_prices:
        print("前20只缺失的股票:", sorted(list(missing_in_prices))[:20])
    
    print(f"在prices.csv中但不在constituents.csv中的股票数量: {len(extra_in_prices)}")
    if extra_in_prices:
        print("额外的股票/指数:", sorted(list(extra_in_prices)))
    
    # 检查price_cache目录
    price_cache_dir = CONFIG.price_cache_dir
    if os.path.exists(price_cache_dir):
        price_files = [f for f in os.listdir(price_cache_dir) if f.endswith('.csv')]
        print(f"price_cache目录中有 {len(price_files)} 个文件")
        
        # 统计股票和指数文件
        stock_files = [f for f in price_files if not f.split('_')[0] in ['000015', '399006', '399321', '399324', '399372', '399374', '399376']]
        index_files = [f for f in price_files if f.split('_')[0] in ['000015', '399006', '399321', '399324', '399372', '399374', '399376']]
        print(f"其中股票文件: {len(stock_files)}, 指数文件: {len(index_files)}")

if __name__ == "__main__":
    diagnose_data_issue()
