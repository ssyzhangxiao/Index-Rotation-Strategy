#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
缓存管理模块
提供灵活的缓存文件清理功能
"""

import os
import sys
from glob import glob
import pandas as pd
from datetime import datetime

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入配置文件
from config import CONFIG, INDEX_LIST

def get_cache_stats():
    """
    获取缓存文件统计信息
    """
    price_cache_dir = CONFIG.price_cache_dir
    
    # 检查目录是否存在
    if not os.path.exists(price_cache_dir):
        print(f"目录 {price_cache_dir} 不存在")
        return None
    
    # 获取目录中的所有CSV文件
    files = glob(os.path.join(price_cache_dir, '*.csv'))
    total_files = len(files)
    
    if total_files == 0:
        print(f"目录 {price_cache_dir} 中没有文件")
        return {
            'total_files': 0,
            'total_size': 0,
            'oldest_file': None,
            'newest_file': None
        }
    
    # 计算总大小和文件日期信息
    total_size = 0
    oldest_time = None
    newest_time = None
    oldest_file = None
    newest_file = None
    
    for file_path in files:
        try:
            # 获取文件大小
            size = os.path.getsize(file_path)
            total_size += size
            
            # 获取文件修改时间
            mod_time = os.path.getmtime(file_path)
            if oldest_time is None or mod_time < oldest_time:
                oldest_time = mod_time
                oldest_file = file_path
            if newest_time is None or mod_time > newest_time:
                newest_time = mod_time
                newest_file = file_path
        except Exception as e:
            print(f"获取文件信息 {file_path} 出现错误: {e}")
    
    return {
        'total_files': total_files,
        'total_size': total_size,
        'oldest_file': oldest_file,
        'newest_file': newest_file,
        'oldest_time': datetime.fromtimestamp(oldest_time) if oldest_time else None,
        'newest_time': datetime.fromtimestamp(newest_time) if newest_time else None
    }

def clean_old_cache(days=30):
    """
    清理指定天数之前的缓存文件
    """
    price_cache_dir = CONFIG.price_cache_dir
    
    # 检查目录是否存在
    if not os.path.exists(price_cache_dir):
        print(f"目录 {price_cache_dir} 不存在")
        return 0
    
    # 计算截止时间
    cutoff_time = datetime.now().timestamp() - (days * 24 * 60 * 60)
    
    # 获取目录中的所有CSV文件
    files = glob(os.path.join(price_cache_dir, '*.csv'))
    total_files = len(files)
    
    if total_files == 0:
        print(f"目录 {price_cache_dir} 中没有文件")
        return 0
    
    print(f"检查 {total_files} 个文件，清理 {days} 天前的缓存...")
    
    # 删除过期文件
    deleted_count = 0
    for file_path in files:
        try:
            # 获取文件修改时间
            mod_time = os.path.getmtime(file_path)
            if mod_time < cutoff_time:
                os.remove(file_path)
                deleted_count += 1
                print(f"已删除过期文件: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"处理文件 {file_path} 出现错误: {e}")
    
    print(f"总共删除了 {deleted_count} 个过期文件")
    return deleted_count

def clean_all_cache():
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

def show_cache_info():
    """
    显示缓存文件信息
    """
    print("=== 价格缓存文件信息 ===")
    stats = get_cache_stats()
    
    if stats is None:
        return
    
    print(f"文件总数: {stats['total_files']}")
    print(f"总大小: {stats['total_size'] / (1024*1024):.2f} MB")
    
    if stats['oldest_time']:
        print(f"最早文件: {os.path.basename(stats['oldest_file'])} ({stats['oldest_time']})")
    
    if stats['newest_time']:
        print(f"最新文件: {os.path.basename(stats['newest_file'])} ({stats['newest_time']})")

def main():
    """
    主函数，提供交互式缓存管理
    """
    while True:
        print("\n=== 缓存管理工具 ===")
        print("1. 显示缓存信息")
        print("2. 清理所有缓存")
        print("3. 清理30天前的缓存")
        print("4. 清理7天前的缓存")
        print("0. 返回上级菜单")
        
        try:
            choice = int(input("请选择操作 (0-4): "))
        except ValueError:
            print("输入无效，请输入数字!")
            continue
        
        if choice == 1:
            show_cache_info()
        elif choice == 2:
            confirm = input("确定要删除所有价格缓存文件吗? (yes/no): ")
            if confirm.lower() in ['yes', 'y']:
                clean_all_cache()
            else:
                print("操作已取消")
        elif choice == 3:
            clean_old_cache(30)
        elif choice == 4:
            clean_old_cache(7)
        elif choice == 0:
            break
        else:
            print("无效选项，请重新选择")

if __name__ == "__main__":
    main()