#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
from glob import glob
import sys
from datetime import datetime

# 添加新函数
def clean_price_cache_only():
    """
    只删除price_cache目录中的所有文件
    """
    price_cache_dir = CONFIG.price_cache_dir
    
    # 检查目录是否存在
    if not os.path.exists(price_cache_dir):
        print(f"目录 {price_cache_dir} 不存在")
        return 0
    
    # 获取目录中的所有CSV文件
    files = glob(os.path.join(price_cache_dir, '*.csv'))
    total_files = len(files)
    
    if total_files == 0:
        print(f"目录 {price_cache_dir} 中没有文件")
        return 0
    
    print(f"找到 {total_files} 个价格缓存文件，准备删除...")
    
    # 删除所有文件
    deleted_count = 0
    for file_path in files:
        try:
            os.remove(file_path)
            deleted_count += 1
            if deleted_count % 50 == 0:  # 每删除50个文件显示一次进度
                print(f"已删除 {deleted_count}/{total_files} 个文件")
        except Exception as e:
            print(f"删除文件 {file_path} 出现错误: {e}")
    
    print(f"总共删除了 {deleted_count}/{total_files} 个价格缓存文件")
    return deleted_count

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入配置文件
from config import CONFIG, INDEX_LIST

def delete_all_price_cache():
    """
    删除所有价格缓存文件
    """
    price_cache_dir = CONFIG.price_cache_dir
    
    # 检查目录是否存在
    if not os.path.exists(price_cache_dir):
        print(f"目录 {price_cache_dir} 不存在")
        return 0
    
    # 获取目录中的所有CSV文件
    files = glob(os.path.join(price_cache_dir, '*.csv'))
    total_files = len(files)
    
    if total_files == 0:
        print(f"目录 {price_cache_dir} 中没有文件")
        return 0
    
    print(f"找到 {total_files} 个价格缓存文件")
    
    # 删除所有文件
    deleted_count = 0
    for file_path in files:
        try:
            os.remove(file_path)
            deleted_count += 1
            if deleted_count % 50 == 0:  # 每删除50个文件显示一次进度
                print(f"已删除 {deleted_count}/{total_files} 个文件")
        except Exception as e:
            print(f"删除文件 {file_path} 出现错误: {e}")
    
    print(f"总共删除了 {deleted_count}/{total_files} 个文件")
    return deleted_count


# 获取所有成分股代码
def get_constituents_stocks():
    constituents_dir = CONFIG.constituents_cache_dir
    all_stocks = set()
    
    # 检查目录是否存在
    if not os.path.exists(constituents_dir):
        print(f"目录 {constituents_dir} 不存在")
        return all_stocks
    
    # 只处理配置中定义的指数成分股文件
    for index_code in INDEX_LIST:
        file_path = os.path.join(constituents_dir, f'constituents_{index_code}.csv')
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path, dtype={'成分股代码': str})  # 确保读取为字符串类型
                if '成分股代码' in df.columns:
                    stocks = df['成分股代码'].unique()
                    # 确保股票代码格式正确（6位，保留前导零）
                    formatted_stocks = set(str(stock).zfill(6) for stock in stocks)
                    all_stocks.update(formatted_stocks)
                else:
                    print(f"文件 {file_path} 中没有找到'成分股代码'列")
            except Exception as e:
                print(f"读取文件 {file_path} 出现错误: {e}")
        else:
            print(f"成分股文件 {file_path} 不存在")
    
    print(f"总共找到 {len(all_stocks)} 个不同的成分股代码")
    return all_stocks

# 检查并清理price_cache目录
def clean_price_cache(constituent_stocks):
    price_cache_dir = CONFIG.price_cache_dir
    removed_count = 0
    checked_count = 0
    
    # 检查目录是否存在
    if not os.path.exists(price_cache_dir):
        print(f"目录 {price_cache_dir} 不存在")
        return removed_count
    
    # 获取目录中的所有CSV文件
    files = glob(os.path.join(price_cache_dir, '*.csv'))
    total_files = len(files)
    
    if total_files == 0:
        print(f"目录 {price_cache_dir} 中没有文件")
        return removed_count
    
    print(f"开始检查 {total_files} 个文件...")
    
    # 创建保护列表，包括成分股代码和指数代码
    # 确保所有代码都是6位格式，保留前导零
    protected_symbols = set(str(stock).zfill(6) for stock in constituent_stocks) | set(INDEX_LIST)
    
    # 遍历所有价格文件
    for file_path in files:
        checked_count += 1
        # 从文件名中提取股票代码（假设文件名格式为 {股票代码}_日期_日期.csv）
        filename = os.path.basename(file_path)
        parts = filename.split('_')
        
        if len(parts) >= 1:
            stock_code = parts[0]
            # 确保股票代码格式正确（6位，保留前导零）
            formatted_stock_code = str(stock_code).zfill(6)
            
            # 如果股票代码不在保护列表中，则删除文件
            if formatted_stock_code not in protected_symbols:
                try:
                    os.remove(file_path)
                    print(f"[{checked_count}/{total_files}] 已删除不匹配的文件: {filename}")
                    removed_count += 1
                except Exception as e:
                    print(f"[{checked_count}/{total_files}] 删除文件 {file_path} 出现错误: {e}")
            else:
                print(f"[{checked_count}/{total_files}] 保留文件: {filename}")
        else:
            print(f"[{checked_count}/{total_files}] 无法解析文件名: {filename}")
    
    print(f"总共检查了 {checked_count} 个文件，删除了 {removed_count} 个不匹配的文件")
    return removed_count

def merge_index_files():
    """
    检查合并将相同指数的文件合并为一个新文件
    文件名中保留开始日期到最新的日期，然后删除合并前的文件
    """
    price_cache_dir = CONFIG.price_cache_dir
    
    # 检查目录是否存在
    if not os.path.exists(price_cache_dir):
        print(f"目录 {price_cache_dir} 不存在")
        return
    
    # 获取目录中的所有CSV文件
    files = glob(os.path.join(price_cache_dir, '*.csv'))
    
    # 按股票代码分组文件
    file_groups = {}
    for file_path in files:
        filename = os.path.basename(file_path)
        parts = filename.split('_')
        if len(parts) >= 3:
            stock_code = parts[0]
            start_date = parts[1]
            end_date = parts[2].replace('.csv', '')
            
            if stock_code not in file_groups:
                file_groups[stock_code] = []
            
            file_groups[stock_code].append({
                'path': file_path,
                'start_date': start_date,
                'end_date': end_date
            })
    
    # 处理每个分组
    merged_count = 0
    for stock_code, file_list in file_groups.items():
        if len(file_list) <= 1:
            # 只有一个文件，不需要合并
            continue
            
        # 找到最早的开始日期和最晚的结束日期
        earliest_start = min(file['start_date'] for file in file_list)
        latest_end = max(file['end_date'] for file in file_list)
        
        # 读取所有文件并合并
        dataframes = []
        files_to_remove = []
        
        for file_info in file_list:
            try:
                df = pd.read_csv(file_info['path'])
                dataframes.append(df)
                files_to_remove.append(file_info['path'])
            except Exception as e:
                print(f"读取文件 {file_info['path']} 出现错误: {e}")
        
        if dataframes:
            # 合并所有数据
            merged_df = pd.concat(dataframes, ignore_index=True)
            
            # 删除重复行（基于日期列）
            if 'date' in merged_df.columns:
                merged_df = merged_df.drop_duplicates(subset=['date'], keep='first')
                merged_df = merged_df.sort_values('date')
            
            # 构造新文件名
            new_filename = f"{stock_code}_{earliest_start}_{latest_end}.csv"
            new_file_path = os.path.join(price_cache_dir, new_filename)
            
            # 保存合并后的文件
            try:
                merged_df.to_csv(new_file_path, index=False)
                print(f"合并文件 {stock_code} 为 {new_filename}")
                
                # 删除原来的文件
                for file_path in files_to_remove:
                    os.remove(file_path)
                    print(f"删除旧文件: {os.path.basename(file_path)}")
                
                merged_count += len(files_to_remove)
            except Exception as e:
                print(f"保存合并文件 {new_filename} 出现错误: {e}")
    
    if merged_count > 0:
        print(f"总共合并了 {merged_count} 个文件")
    else:
        print("没有需要合并的文件")

def main(price_cache_only=False):
    if price_cache_only:
        print("开始删除price_cache目录中的所有文件...")
        deleted_count = clean_price_cache_only()
        print(f"删除完成，共删除了 {deleted_count} 个文件")
        return
    
    print("开始检查并清理price_cache目录...")
    
    # 获取所有成分股代码
    constituent_stocks = get_constituents_stocks()
    
    # 如果没有成分股数据，直接清理所有价格缓存文件
    if len(constituent_stocks) == 0:
        print("未找到成分股代码，将清理所有价格缓存文件...")
        delete_all_price_cache()
        return
    
    # 清理price_cache目录
    removed_count = clean_price_cache(constituent_stocks)
    
    # 合并相同指数的文件
    print("\n检查合并将相同指数的文件合并为一个新文件文件名中保留开始日期到最新的日期，然后删除合并前的文件")
    merge_index_files()
    
    print(f"清理完成，共删除了 {removed_count} 个文件")


if __name__ == "__main__":
    # 检查命令行参数
    if len(sys.argv) > 1 and sys.argv[1] == "--price-cache-only":
        main(price_cache_only=True)
    else:
        main()