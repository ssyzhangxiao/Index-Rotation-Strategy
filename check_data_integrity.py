#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据完整性检查脚本
用于检查指数成分股数据的完整性，并生成完整的数据信息报告
"""

import os
import pandas as pd
from datetime import datetime
from config import CONFIG, STYLE_INDEX_SYMBOLS

def check_constituents_data():
    """检查成分股数据完整性"""
    print("=" * 60)
    print("指数成分股数据完整性检查报告")
    print("=" * 60)
    
    constituents_dir = CONFIG.constituents_cache_dir
    price_cache_dir = CONFIG.price_cache_dir
    data_dir = CONFIG.data_dir
    
    print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"成分股数据目录: {constituents_dir}")
    print(f"价格数据目录: {price_cache_dir}")
    print()
    
    # 检查目录是否存在
    if not os.path.exists(constituents_dir):
        print(f"错误: 成分股数据目录 {constituents_dir} 不存在")
        return
    
    if not os.path.exists(price_cache_dir):
        print(f"错误: 价格数据目录 {price_cache_dir} 不存在")
        return
    
    # 检查每个指数的成分股文件
    total_stocks = set()
    index_reports = []
    
    for index_code, index_name in STYLE_INDEX_SYMBOLS.items():
        file_path = os.path.join(constituents_dir, f'constituents_{index_code}.csv')
        
        if not os.path.exists(file_path):
            print(f"❌ 缺失: {index_name}({index_code}) 成分股文件")
            index_reports.append({
                'index_code': index_code,
                'index_name': index_name,
                'status': '缺失文件',
                'stock_count': 0,
                'latest_date': 'N/A'
            })
            continue
        
        try:
            # 读取成分股文件
            df = pd.read_csv(file_path)
            
            if '成分股代码' not in df.columns:
                print(f"❌ 错误: {index_name}({index_code}) 文件缺少'成分股代码'列")
                index_reports.append({
                    'index_code': index_code,
                    'index_name': index_name,
                    'status': '缺少列',
                    'stock_count': 0,
                    'latest_date': 'N/A'
                })
                continue
            
            # 获取成分股代码
            stocks = df['成分股代码'].dropna().unique()
            stock_count = len(stocks)
            total_stocks.update(stocks)
            
            # 获取最新日期
            latest_date = 'N/A'
            if '日期' in df.columns and not df['日期'].empty:
                try:
                    latest_date = pd.to_datetime(df['日期']).max().strftime('%Y-%m-%d')
                except:
                    latest_date = '日期格式错误'
            
            print(f"✅ 正常: {index_name}({index_code}) - {stock_count}只成分股 (最新日期: {latest_date})")
            index_reports.append({
                'index_code': index_code,
                'index_name': index_name,
                'status': '正常',
                'stock_count': stock_count,
                'latest_date': latest_date
            })
            
        except Exception as e:
            print(f"❌ 错误: {index_name}({index_code}) 文件读取失败 - {str(e)}")
            index_reports.append({
                'index_code': index_code,
                'index_name': index_name,
                'status': f'读取错误: {str(e)}',
                'stock_count': 0,
                'latest_date': 'N/A'
            })
    
    print()
    print("-" * 60)
    print("指数成分股数据汇总")
    print("-" * 60)
    print(f"指数总数: {len(STYLE_INDEX_SYMBOLS)}")
    print(f"正常指数数: {sum(1 for r in index_reports if r['status'] == '正常')}")
    print(f"总成分股数(去重): {len(total_stocks)}")
    
    # 显示各指数成分股数量详情
    print("\n各指数成分股详情:")
    for report in index_reports:
        status_icon = "✅" if report['status'] == '正常' else "❌"
        print(f"  {status_icon} {report['index_name']}({report['index_code']}): {report['stock_count']}只成分股")
    
    # 检查价格数据目录
    print()
    print("-" * 60)
    print("价格数据检查")
    print("-" * 60)
    
    if os.path.exists(price_cache_dir):
        price_files = [f for f in os.listdir(price_cache_dir) if f.endswith('.csv')]
        print(f"价格数据文件数: {len(price_files)}")
        
        # 统计指数价格文件
        index_price_files = [f for f in price_files if any(f.startswith(idx) for idx in STYLE_INDEX_SYMBOLS.keys())]
        stock_price_files = [f for f in price_files if f not in index_price_files]
        
        print(f"其中指数价格文件: {len(index_price_files)}")
        print(f"其中股票价格文件: {len(stock_price_files)}")
        
        # 检查是否有合并文件
        prices_csv = os.path.join(data_dir, 'prices.csv')
        constituents_csv = os.path.join(data_dir, 'constituents.csv')
        
        print()
        print("-" * 60)
        print("合并数据文件检查")
        print("-" * 60)
        if os.path.exists(prices_csv):
            try:
                prices_df = pd.read_csv(prices_csv)
                print(f"✅ prices.csv 存在 - {len(prices_df)} 条记录")
                if not prices_df.empty:
                    symbols = prices_df['symbol'].unique() if 'symbol' in prices_df.columns else []
                    print(f"  包含 {len(symbols)} 个标的")
                    
                    # 统计指数和股票数量
                    index_symbols = [s for s in symbols if s in STYLE_INDEX_SYMBOLS.keys()]
                    stock_symbols = [s for s in symbols if s not in STYLE_INDEX_SYMBOLS.keys()]
                    print(f"  其中指数: {len(index_symbols)} 个")
                    print(f"  其中股票: {len(stock_symbols)} 个")
            except Exception as e:
                print(f"❌ prices.csv 读取失败: {e}")
        else:
            print("⚠️  prices.csv 文件不存在")
            
        if os.path.exists(constituents_csv):
            try:
                constituents_df = pd.read_csv(constituents_csv)
                print(f"✅ constituents.csv 存在 - {len(constituents_df)} 条记录")
            except Exception as e:
                print(f"❌ constituents.csv 读取失败: {e}")
        else:
            print("⚠️  constituents.csv 文件不存在")
    else:
        print("价格数据目录不存在")
    
    print()
    print("=" * 60)
    print("数据完整性检查完成")
    print("=" * 60)

if __name__ == "__main__":
    check_constituents_data()