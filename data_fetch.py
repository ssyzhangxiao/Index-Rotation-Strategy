def fix_price_cache_headers():
    """
    批量修正 price_cache 下所有 csv 文件的表头，确保字段顺序一致。
    标准表头：['date','open','high','low','close','volume','symbol']
    对于指数文件如000015，自动适配其表头；对于无表头的文件，强制加上标准表头。
    """
    import glob
    import shutil
    price_cache_dir = PRICE_CACHE_DIR if 'PRICE_CACHE_DIR' in globals() else './data/price_cache'
    files = glob.glob(os.path.join(price_cache_dir, '*.csv'))
    # 以000015或第一个有表头的文件为标准
    standard_header = None
    for f in files:
        with open(f, 'r', encoding='utf-8') as fin:
            first_line = fin.readline().strip()
            if 'date' in first_line and 'symbol' in first_line:
                standard_header = first_line
                break
    # 若没找到标准表头，使用最常见的股票表头
    if not standard_header:
        standard_header = 'date,open,high,low,close,volume,symbol'
    for f in files:
        with open(f, 'r', encoding='utf-8') as fin:
            first_line = fin.readline().strip()
            # 如果已经有标准表头且字段数一致则跳过
            if first_line == standard_header:
                continue
            # 如果第一行不是表头（如全是数字），则加表头
            if not first_line or not first_line.replace(',', '').replace('.', '').replace('-', '').replace(':', '').isalnum() or 'date' in first_line:
                # 已有表头但不标准，强制替换
                lines = fin.readlines()
                content = [standard_header + '\n'] + lines
            else:
                # 没有表头，需加表头
                fin.seek(0)
                content = [standard_header + '\n'] + fin.readlines()
        # 备份原文件
        shutil.copy(f, f + '.bak')
        with open(f, 'w', encoding='utf-8') as fout:
            fout.writelines(content)
    print(f"已修正 {len(files)} 个csv文件的表头为: {standard_header}")
# 读取 price_cache 目录下所有单股票/指数 csv 文件并合并
def read_all_price_cache_df():
    """
    读取 price_cache 目录下所有单股票/指数 csv 文件并合并为一个 DataFrame。
    Returns:
        pd.DataFrame: 合并后的所有价格数据
    """
    import glob
    files = glob.glob(os.path.join(PRICE_CACHE_DIR, '*.csv'))
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f, dtype=str)
            # 针对 000015 这类指数文件，自动修正字段名
            if '指数代码' in df.columns:
                # 重命名为标准字段
                rename_map = {
                    '指数代码': 'symbol',
                    'open': 'open',
                    'high': 'high',
                    'low': 'low',
                    'close': 'close',
                    'date': 'date',
                    'volume': 'volume',
                }
                # 只保留标准字段
                keep_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']
                # 先重命名
                df = df.rename(columns=rename_map)
                # 补齐 volume 字段
                if 'volume' not in df.columns:
                    if '成交量' in df.columns:
                        df['volume'] = df['成交量']
                    else:
                        df['volume'] = 0
                # 补齐 symbol 字段
                if 'symbol' not in df.columns and '指数代码' in df.columns:
                    df['symbol'] = df['指数代码']
                # 只保留标准字段
                df = df[[col for col in keep_cols if col in df.columns]]
            # 统一类型
            df['symbol'] = df['symbol'].astype(str)
            dfs.append(df)
        except Exception as e:
            print(f"读取 {f} 失败: {e}")
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)
from data_source import CustomDataSource
custom_data_source = CustomDataSource()
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Fetcher for Chinese A-share stocks
Supports downloading index and stock data with caching and batch processing
"""

import os
import time
import pandas as pd
import glob
import akshare as ak
import warnings
import numpy as np
import json
from logger import setup_logger
from file_utils import record_failure, save_download_log, load_download_log, clear_failure_files
from datetime import datetime, timedelta
from tqdm import tqdm
from file_utils import load_pending_stocks, load_failed_tasks, save_pending_stocks, save_batch_state, load_batch_state, clear_batch_state
from config import CONFIG, STYLE_INDEX_SYMBOLS

# 忽略OpenPyxl警告
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# 配置基本路径
DATA_DIR = CONFIG.data_dir
CONSTITUENTS_CACHE_DIR = CONFIG.constituents_cache_dir
PRICE_CACHE_DIR = CONFIG.price_cache_dir
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CONSTITUENTS_CACHE_DIR, exist_ok=True)
os.makedirs(PRICE_CACHE_DIR, exist_ok=True)

# prices.csv 分割相关
PRICES_BASE = os.path.join(DATA_DIR, "prices")
PRICES_MAX_MB = 50
def get_prices_part_files():
    """获取所有分段prices文件，按顺序返回。"""
    files = sorted(glob.glob(f"{PRICES_BASE}_*.csv"), key=lambda x: int(x.split('_')[-1].split('.')[0]))
    return files

def get_next_prices_file():
    """获取当前可写入的prices分段文件名。"""
    files = get_prices_part_files()
    if not files:
        return f"{PRICES_BASE}_1.csv"
    last_file = files[-1]
    size_mb = os.path.getsize(last_file) / 1024 / 1024
    if size_mb >= PRICES_MAX_MB:
        idx = int(last_file.split('_')[-1].split('.')[0]) + 1
        return f"{PRICES_BASE}_{idx}.csv"
    return last_file

def save_prices_df(df):
    """保存DataFrame到分段prices文件，自动分割。"""
    file = get_next_prices_file()
    write_header = not os.path.exists(file) or os.path.getsize(file) == 0
    df.to_csv(file, mode='a', header=write_header, index=False)

def read_all_prices_df():
    """读取所有分段prices文件并合并为一个DataFrame。"""
    files = get_prices_part_files()
    if not files:
        return pd.DataFrame()
    dfs = [pd.read_csv(f, parse_dates=['date']) for f in files]
    return pd.concat(dfs, ignore_index=True)

# 配置参数
start_date_str = CONFIG.data_download_start_date
end_date_str = CONFIG.data_download_end_date

# 行业信息缓存
INDUSTRY_CACHE = {}


def get_stock_industry(symbol):
    """
    获取股票的行业信息
    
    Args:
        symbol (str): 股票代码
        
    Returns:
        str: 股票所属行业
    """
    # 检查缓存中是否已有该股票的行业信息
    if symbol in INDUSTRY_CACHE:
        return INDUSTRY_CACHE[symbol]
    
    try:
        # 使用akshare获取股票信息
        stock_info = ak.stock_individual_info_em(symbol=symbol)
        
        # 从返回的数据中提取行业信息
        if not stock_info.empty:
            industry_row = stock_info[stock_info['item'] == '行业']
            if not industry_row.empty:
                industry = industry_row['value'].iloc[0]
                # 缓存行业信息
                INDUSTRY_CACHE[symbol] = industry
                return industry
        
        # 如果没有找到行业信息，返回默认值
        return "未知"
    except Exception as e:
        logger.warning(f"获取股票{symbol}行业信息失败: {e}")
        return "未知"

# 策略参数配置
strategy_params = {
    'initial_cash': None,
    'fee_amount': None,
    'start_date': None,
    'end_date': None
}
MAX_RETRIES = 5  # 增加重试次数
REQUEST_DELAY = 1
MIN_HISTORY_DAYS = 100
BATCH_WAIT_MINUTES = 5  # 批次间等待5分钟，避免限流

# 日志配置
logger = setup_logger(os.path.join(DATA_DIR, "data_fetcher.log"))

# 定义失败记录文件路径
FAILED_INDEX_FILE = os.path.join(DATA_DIR, "failed_indexes.txt")
FAILED_STOCKS_FILE = os.path.join(DATA_DIR, "failed_stocks.txt")
PENDING_STOCKS_FILE = os.path.join(DATA_DIR, "pending_stocks.txt")
BATCH_STATE_FILE = os.path.join(DATA_DIR, "batch_state.txt")
DOWNLOAD_LOG_FILE = os.path.join(DATA_DIR, "download_log.json")  # 添加下载日志文件路径

def get_cache_filename(symbol, data_type="price"):
    """生成缓存文件名"""
    # 统一符号格式，去除任何后缀
    clean_symbol = str(symbol).replace('.SH', '').replace('.SZ', '')
    
    if data_type == "price":
        return os.path.join(PRICE_CACHE_DIR, f"{clean_symbol}_{start_date_str}_{end_date_str}.csv")
    elif data_type == "constituents":
        return os.path.join(CONSTITUENTS_CACHE_DIR, f"constituents_{clean_symbol}.csv")
    # 确保对于未知的data_type也返回默认的price类型文件名，而不是None
    return os.path.join(PRICE_CACHE_DIR, f"{clean_symbol}_{start_date_str}_{end_date_str}.csv")

def clean_non_constituent_stocks(constituent_symbols):
    """
    清理不在当前成分股列表中的股票数据文件
    
    Args:
        constituent_symbols (list): 当前成分股列表
    """
    logger.info("开始清理非成分股数据文件...")
    cleaned_count = 0
    
    # 确保成分股代码格式正确
    formatted_constituents = set(str(symbol).zfill(6) for symbol in constituent_symbols)
    
    # 添加指数代码到保护列表，避免误删
    protected_symbols = set(STYLE_INDEX_SYMBOLS.keys())
    
    # 遍历价格缓存目录中的所有文件
    if os.path.exists(PRICE_CACHE_DIR):
        for filename in os.listdir(PRICE_CACHE_DIR):
            if filename.endswith(".csv") and "_" in filename:
                # 提取股票代码 (假设文件名格式为 {symbol}_{start_date}_{end_date}.csv)
                symbol = filename.split("_")[0]
                
                # 如果股票代码不在当前成分股列表中，且不是受保护的指数代码，则删除该文件
                if symbol not in formatted_constituents and symbol not in protected_symbols:
                    file_path = os.path.join(PRICE_CACHE_DIR, filename)
                    try:
                        os.remove(file_path)
                        logger.info(f"已清理非成分股数据文件: {filename}")
                        cleaned_count += 1
                    except Exception as e:
                        logger.warning(f"删除非成分股数据文件失败 {filename}: {e}")
    
    logger.info(f"清理完成，共删除 {cleaned_count} 个非成分股数据文件")

def get_last_date_in_cache(cache_file):
    """获取缓存文件中的最后日期"""
    last_date = None
    
    # 首先检查缓存文件
    if os.path.exists(cache_file):
        try:
            df = pd.read_csv(cache_file, parse_dates=['date'])
            if not df.empty:
                last_date = df['date'].max()
                # 转换为日期字符串，去掉时间部分
                return last_date.strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"读取缓存文件 {cache_file} 时出错: {e}")
    
    # 如果缓存文件不存在或为空，则检查分段prices文件
    try:
        symbol = os.path.basename(cache_file).split('_')[0]
        df = read_all_prices_df()
        if not df.empty:
            symbol_df = df[df['symbol'] == symbol]
            if not symbol_df.empty:
                merged_last_date = symbol_df['date'].max()
                if last_date is None or merged_last_date > pd.to_datetime(last_date):
                    logger.info(f"从分段prices文件中找到 {symbol} 的更新数据 (最后日期: {merged_last_date.strftime('%Y-%m-%d')})")
                    return merged_last_date.strftime('%Y-%m-%d')
    except Exception as e:
        logger.warning(f"读取分段prices文件时出错: {e}")
    
    return last_date
def need_full_download(prices_path, index_symbols, end_date_str):
    """
    检查prices.csv是否存在且数据是否覆盖到最新end_date_str。
    若不存在或数据不全，则返回True（需要全新下载）。
    """
    # 检查分段prices文件
    files = get_prices_part_files()
    if not files:
        logger.info("prices分段文件不存在，需要全新下载。")
        return True
    try:
        df = read_all_prices_df()
        for symbol in index_symbols:
            symbol_df = df[df['symbol'] == symbol]
            if symbol_df.empty:
                logger.info(f"prices分段文件缺少指数 {symbol} 的数据，需要全新下载。")
                return True
            max_date = symbol_df['date'].max()
            if pd.to_datetime(max_date) < pd.to_datetime(end_date_str):
                logger.info(f"指数 {symbol} 数据未覆盖到 {end_date_str}，需要增量下载。")
                return False  # 只需增量
        logger.info("prices分段文件已包含全部指数的最新数据，无需下载。")
        return False
    except Exception as e:
        logger.warning(f"检查prices分段文件时出错: {e}，默认全新下载。")
        return True
def has_trading_days(start_date, end_date):
    """
    检查两个日期之间是否有交易日
    AKShare接口在日期范围内没有交易日时会返回空DataFrame
    """
    # 首先检查日期是否有效
    if start_date > end_date:
        return False
    
    # 尝试获取上证指数的交易日历（作为A股代表）
    try:
        # 使用上证指数代码 "000001"
        df = ak.stock_zh_a_hist(
            symbol="000001", 
            period="daily",
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", "")
        )
        return not df.empty
    except:
        # 如果获取失败，默认有交易日
        return True

def download_index_data(symbol, retries=MAX_RETRIES):
    """下载单个指数数据，支持增量下载和交易日检查"""
    # 统一符号格式，去除任何后缀
    clean_symbol = symbol.replace('.SH', '').replace('.SZ', '')
    cache_file = get_cache_filename(clean_symbol)
    
    # 检查缓存
    existing_data = pd.DataFrame()
    last_date = None
    
    # 首先检查缓存文件
    if os.path.exists(cache_file):
        try:
            existing_data = pd.read_csv(cache_file, parse_dates=['date'])
            if not existing_data.empty:
                # 获取缓存中的最后日期
                last_date = existing_data['date'].max()
                logger.info(f"找到缓存数据: {clean_symbol} (最后日期: {last_date.strftime('%Y-%m-%d')})")
        except Exception as e:
            logger.warning(f"读取缓存失败 {clean_symbol}: {e}")
    
    # 如果缓存文件不存在或为空，则检查分段prices文件
    if (not os.path.exists(cache_file) or existing_data.empty) and not last_date:
        try:
            df = read_all_prices_df()
            symbol_df = df[df['symbol'] == clean_symbol]
            if not symbol_df.empty:
                existing_data = symbol_df.copy()
                last_date = existing_data['date'].max()
                logger.info(f"从分段prices文件中找到 {clean_symbol} 的数据 (最后日期: {last_date.strftime('%Y-%m-%d')})")
        except Exception as e:
            logger.warning(f"读取分段prices文件失败 {clean_symbol}: {e}")
    
    # 确定开始日期
    actual_start_date = start_date_str
    actual_end_date = end_date_str
    
    if last_date:
        # 增量下载：从最后日期的下一天开始
        actual_start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 检查是否有交易日
        if not has_trading_days(actual_start_date, actual_end_date):
            logger.info(f"指数 {clean_symbol} 在 {actual_start_date} 到 {actual_end_date} 之间无交易日，无需下载")
            return existing_data
            
        logger.info(f"增量下载指数数据: {clean_symbol} (从 {actual_start_date} 到 {actual_end_date})")
    else:
        # 全新下载
        actual_start_date = start_date_str
        actual_end_date = end_date_str
        logger.info(f"全新下载指数数据: {clean_symbol} (从 {actual_start_date} 到 {actual_end_date})")
    
    # 如果开始日期大于结束日期，则无需下载
    if last_date and last_date >= datetime.strptime(actual_end_date, '%Y-%m-%d'):
        logger.info(f"指数 {clean_symbol} 数据已是最新，无需下载")
        return existing_data
    
    # 下载新数据
    new_data = pd.DataFrame()
    for attempt in range(1, retries + 1):
        try:
            # 对于中证指数000015，使用特定接口
            if symbol == "000015":
                # 使用中证指数专用接口
                df = ak.stock_zh_index_hist_csindex(
                    symbol=symbol,
                    start_date=actual_start_date.replace("-", ""),
                    end_date=actual_end_date.replace("-", "")
                )
                # 统一列名
                if not df.empty:
                    # 检查实际返回的列数并适配
                    logger.debug(f"中证指数 {symbol} 返回的列名: {list(df.columns)}")
                    actual_columns = list(df.columns)
                    
                    # 确保至少有需要的基本列
                    required_columns_map = {
                        "日期": "date",
                        "开盘": "open", 
                        "最高": "high",
                        "最低": "low",
                        "收盘": "close",
                        "涨跌": "change",
                        "涨跌幅": "change_percent",
                        "成交量": "volume",
                        "成交金额": "amount"
                    }
                    
                    # 只重命名存在的列
                    rename_dict = {}
                    for col, new_name in required_columns_map.items():
                        if col in actual_columns:
                            rename_dict[col] = new_name
                    
                    df = df.rename(columns=rename_dict)
                    
                    # 添加symbol列
                    df["symbol"] = clean_symbol
                    new_data = df
            # 对于国证指数，使用index_hist_cni接口
            elif symbol.startswith("399"):
                # 使用国证指数专用接口
                df = ak.index_hist_cni(
                    symbol=symbol,
                    start_date=actual_start_date.replace("-", ""),
                    end_date=actual_end_date.replace("-", "")
                )
                # 统一列名
                if not df.empty:
                    # 检查实际返回的列数并适配
                    logger.debug(f"国证指数 {symbol} 返回的列名: {list(df.columns)}")
                    actual_columns = list(df.columns)
                    
                    # 确保至少有需要的基本列
                    required_columns_map = {
                        "日期": "date",
                        "开盘价": "open",
                        "最高价": "high",
                        "最低价": "low",
                        "收盘价": "close",
                        "涨跌幅": "change_percent",
                        "成交量": "volume",
                        "成交额": "amount"
                    }
                    
                    # 只重命名存在的列
                    rename_dict = {}
                    for col, new_name in required_columns_map.items():
                        if col in actual_columns:
                            rename_dict[col] = new_name
                    
                    df = df.rename(columns=rename_dict)
                    
                    # 添加symbol列
                    df["symbol"] = clean_symbol
                    
                    # 国证指数成交量单位为万手，需要转换为普通手
                    if "volume" in df.columns:
                        df["volume"] = df["volume"] * 10000
                    
                    new_data = df
            else:
                new_data = ak.stock_zh_index_daily_em(symbol=symbol, start_date=actual_start_date, end_date=actual_end_date)
                # 确保新数据中的symbol列使用clean_symbol
                if not new_data.empty:
                    new_data["symbol"] = clean_symbol
            break
        except Exception as e:
            logger.warning(f"第 {attempt} 次尝试下载 {symbol} 数据失败: {e}")
            if attempt < retries:
                time.sleep(5)
            else:
                raise Exception(f"下载 {symbol} 数据失败，已达到最大重试次数")

            if attempt < MAX_RETRIES:
                time.sleep(REQUEST_DELAY * 2 ** attempt)
    
    # 合并新旧数据
    if not new_data.empty:
        # 转换日期格式
        new_data['date'] = pd.to_datetime(new_data['date'])
        
        if not existing_data.empty:
            # 合并数据，并去重
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
            combined_data = combined_data.sort_values('date')
            combined_data = combined_data.drop_duplicates(subset=['date'], keep='last')
            
            # 保存到缓存
            combined_data.to_csv(cache_file, index=False)
            logger.info(f"合并增量数据: {symbol} (新增 {len(new_data)} 条记录, 总记录 {len(combined_data)})")
            return combined_data
        else:
            # 保存新数据
            new_data.to_csv(cache_file, index=False)
            logger.info(f"保存新数据: {symbol} (新增 {len(new_data)} 条记录)")
            return new_data
    elif not existing_data.empty:
        logger.info(f"没有新数据可下载，使用缓存数据: {symbol}")
        return existing_data
    
    logger.error(f"指数 {symbol} 下载失败，已达到最大重试次数")
    record_failure(symbol, "index")
    return pd.DataFrame()

def download_stock_data(symbol, retries=MAX_RETRIES):
    """下载单个股票数据，支持增量下载和交易日检查"""
    # 确保symbol是字符串类型，仅对纯数字代码补零到6位
    symbol = str(symbol)
    if symbol.isdigit():
        symbol = symbol.zfill(6)
    cache_file = get_cache_filename(symbol)
    
    # 检查缓存
    existing_data = pd.DataFrame()
    last_date = None
    
    # 首先检查缓存文件
    if os.path.exists(cache_file):
        try:
            existing_data = pd.read_csv(cache_file, parse_dates=['date'])
            if not existing_data.empty:
                # 获取缓存中的最后日期
                last_date = existing_data['date'].max()
                logger.info(f"找到缓存数据: {symbol} (最后日期: {last_date.strftime('%Y-%m-%d')})")
        except Exception as e:
            logger.warning(f"读取缓存失败 {symbol}: {e}")
    
    # 如果缓存文件不存在或为空，则检查合并文件(prices.csv)
    if (not os.path.exists(cache_file) or existing_data.empty) and not last_date:
        prices_file = os.path.join(DATA_DIR, "prices.csv")
        if os.path.exists(prices_file):
            try:
                df = pd.read_csv(prices_file, parse_dates=['date'])
                # 筛选出该股票的数据
                symbol_df = df[df['symbol'] == symbol]
                if not symbol_df.empty:
                    existing_data = symbol_df.copy()
                    last_date = existing_data['date'].max()
                    logger.info(f"从合并文件中找到 {symbol} 的数据 (最后日期: {last_date.strftime('%Y-%m-%d')})")
            except Exception as e:
                logger.warning(f"读取合并文件失败 {symbol}: {e}")
    
    # 确定开始日期
    actual_start_date = start_date_str
    actual_end_date = end_date_str
    
    if last_date:
        # 增量下载：从最后日期的下一天开始
        actual_start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 检查是否有交易日
        if not has_trading_days(actual_start_date, actual_end_date):
            logger.info(f"股票 {symbol} 在 {actual_start_date} 到 {actual_end_date} 之间无交易日，无需下载")
            return existing_data
            
        logger.info(f"增量下载股票数据: {symbol} (从 {actual_start_date} 到 {actual_end_date})")
    else:
        # 全新下载
        actual_start_date = start_date_str
        actual_end_date = end_date_str
        logger.info(f"全新下载股票数据: {symbol} (从 {actual_start_date} 到 {actual_end_date})")
    
    # 如果开始日期大于结束日期，则无需下载
    if last_date and last_date >= datetime.strptime(actual_end_date, '%Y-%m-%d'):
        logger.info(f"股票 {symbol} 数据已是最新，无需下载")
        return existing_data
    
    # 下载新数据
    new_data = pd.DataFrame()
    for attempt in range(1, retries + 1):
        try:
            # 尝试两种不同的接口
            try:
                # 第一种接口 - 使用 stock_zh_a_hist (返回中文列名)
                logger.debug(f"尝试使用 stock_zh_a_hist 接口下载 {symbol} 数据 (尝试 {attempt}/{MAX_RETRIES})")
                df = ak.stock_zh_a_hist(
                    symbol=symbol, 
                    period="daily",
                    start_date=actual_start_date.replace("-", ""),
                    end_date=actual_end_date.replace("-", ""),
                    adjust="hfq"
                )
                
                # 验证返回数据格式
                if df is not None and not df.empty:
                    logger.debug(f"stock_zh_a_hist 接口返回有效数据，记录数: {len(df)}")
                    # 处理第一种接口返回的中文列名
                    df = df.rename(columns={
                        "日期": "date",
                        "股票代码": "symbol",
                        "开盘": "open",
                        "收盘": "close",
                        "最高": "high",
                        "最低": "low",
                        "成交量": "volume",
                        "成交额": "amount",
                        "振幅": "amplitude",
                        "涨跌幅": "change_percent",
                        "涨跌额": "change_amount",
                        "换手率": "turnover_rate"
                    })
                else:
                    logger.warning(f"stock_zh_a_hist 接口返回数据为空或格式不正确，尝试第二种接口")
                    raise ValueError("数据为空或格式不正确")
                    
            except Exception as e:
                logger.warning(f"使用第一种接口失败 ({e})，尝试第二种接口")
                # 第二种接口 - 使用 stock_zh_a_daily (返回英文列名)
                exchange_prefix = "sz" if symbol.startswith("00") or symbol.startswith("30") else "sh"
                logger.debug(f"尝试使用 stock_zh_a_daily 接口下载 {symbol} 数据")
                df = ak.stock_zh_a_daily(
                    symbol=f"{exchange_prefix}{symbol}",
                    adjust="hfq",
                    start_date=actual_start_date,
                    end_date=actual_end_date
                )
                
                # 验证第二种接口返回数据
                if df is not None and not df.empty:
                    logger.debug(f"stock_zh_a_daily 接口返回有效数据，记录数: {len(df)}")
                    # 第二种接口已返回英文列名，不需要重命名
                else:
                    logger.warning(f"第二种接口返回数据为空或格式不正确")
                    raise ValueError("数据为空或格式不正确")
            
            if df is not None and not df.empty:
                # 确保必要字段存在
                required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    logger.warning(f"返回数据缺少必要列: {missing_columns}")
                    raise ValueError(f"返回数据缺少必要列: {missing_columns}")
                
                # 转换日期格式
                df['date'] = pd.to_datetime(df['date'])
                
                # 确保股票代码存在
                if 'symbol' not in df.columns:
                    df['symbol'] = symbol
                
                new_data = df
                logger.info(f"{symbol} 数据下载成功，获取记录数: {len(new_data)}")
                break
                
        except Exception as e:
            logger.error(f"下载股票 {symbol} 失败 (尝试 {attempt}/{MAX_RETRIES}): {e}", exc_info=True)
            
            # 如果是网络相关错误，可以尝试不同的接口
            if attempt < MAX_RETRIES and ("网络" in str(e) or "连接" in str(e)):
                logger.info(f"尝试切换数据接口...")
            elif attempt < MAX_RETRIES:
                logger.info(f"等待 {REQUEST_DELAY * 2 ** attempt:.1f} 秒后重试...")
                
            if attempt < MAX_RETRIES:
                time.sleep(REQUEST_DELAY * 2 ** attempt)
    
    # 合并新旧数据
    if not new_data.empty:
        # 转换日期格式
        new_data['date'] = pd.to_datetime(new_data['date'])
        
        # 确保symbol列是字符串类型，保留前导零
        new_data['symbol'] = new_data['symbol'].astype(str)
        
        if not existing_data.empty:
            # 合并数据，并去重
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
            combined_data = combined_data.sort_values('date')
            combined_data = combined_data.drop_duplicates(subset=['date'], keep='last')
            
            # 确保symbol列是字符串类型，保留前导零
            combined_data['symbol'] = combined_data['symbol'].astype(str)
            
            # 保存到缓存
            combined_data.to_csv(cache_file, index=False)
            logger.info(f"合并增量数据: {symbol} (新增 {len(new_data)} 条记录, 总记录 {len(combined_data)})")
            return combined_data
        else:
            # 保存新数据
            # 确保symbol列是字符串类型，保留前导零
            new_data['symbol'] = new_data['symbol'].astype(str)
            new_data.to_csv(cache_file, index=False)
            logger.info(f"保存新数据: {symbol} (新增 {len(new_data)} 条记录)")
            return new_data
    elif not existing_data.empty:
        logger.info(f"没有新数据可下载，使用缓存数据: {symbol}")
        # 确保symbol列是字符串类型，保留前导零
        existing_data['symbol'] = existing_data['symbol'].astype(str)
        return existing_data
    
    logger.error(f"股票 {symbol} 下载失败，已达到最大重试次数")
    record_failure(symbol, "stock")
    return pd.DataFrame()

def get_constituents(symbol, retries=MAX_RETRIES):
    """获取指数成分股，带有缓存和重试机制"""
    cache_file = get_cache_filename(symbol, "constituents")
    
    # 检查缓存
    if os.path.exists(cache_file):
        try:
            # 确保读取时成分股代码为字符串类型
            df = pd.read_csv(cache_file, dtype={"成分股代码": str})
            # 再次确保代码格式正确
            df["成分股代码"] = df["成分股代码"].astype(str).str.zfill(6)
            constituents = df["成分股代码"].tolist()
            latest_date = pd.to_datetime(df["日期"].iloc[0])
            logger.info(f"使用缓存成分股: {symbol} ({len(constituents)}只股票)")
            return constituents, latest_date
        except Exception as e:
            logger.warning(f"读取成分股缓存失败 {symbol}: {e}")
    
    # 下载新数据
    for attempt in range(1, retries + 1):
        try:
            # 对于中证指数000015，使用特定接口
            if symbol == "000015":
                try:
                    constituents_df = ak.index_stock_cons_csindex(symbol=symbol)
                except Exception as e:
                    logger.warning(f"使用index_stock_cons_csindex接口失败，尝试备用接口: {e}")
                    # 尝试使用其他接口作为备选方案
                    constituents_df = ak.index_detail_hist_cni(symbol=symbol)
            else:
                constituents_df = ak.index_detail_hist_cni(symbol=symbol)
            
            if not constituents_df.empty:
                # 处理日期
                constituents_df["日期"] = pd.to_datetime(constituents_df["日期"])
                latest_date = constituents_df["日期"].max()
                
                # 筛选最新成分股
                latest_constituents = constituents_df[constituents_df["日期"] == latest_date].copy()
                
                # 提取成分股代码并标准化格式
                if symbol == "000015":
                    # 中证指数成分股数据格式不同，需要特殊处理
                    if "品种代码" in latest_constituents.columns:
                        latest_constituents["成分股代码"] = latest_constituents["品种代码"].astype(str).str.zfill(6)
                    elif "样本代码" in latest_constituents.columns:
                        latest_constituents["成分股代码"] = latest_constituents["样本代码"].astype(str).str.zfill(6)
                    elif "成分券代码" in latest_constituents.columns:
                        latest_constituents["成分股代码"] = latest_constituents["成分券代码"].astype(str).str.zfill(6)
                    else:
                        logger.error(f"无法识别的列名，当前列: {latest_constituents.columns.tolist()}")
                        raise ValueError("无法识别成分股代码列名")
                else:
                    latest_constituents["成分股代码"] = latest_constituents["样本代码"].astype(str).str.zfill(6)
                
                constituents = latest_constituents["成分股代码"].unique().tolist()
                
                # 保存到缓存
                latest_constituents[["日期", "成分股代码"]].to_csv(cache_file, index=False)
                return constituents, latest_date
            else:
                logger.warning(f"获取到的成分股数据为空: {symbol}")
        except Exception as e:
            logger.error(f"获取成分股失败 (尝试 {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(REQUEST_DELAY * 2 ** attempt)
    
    # 如果所有尝试都失败，记录错误并返回空结果
    logger.error(f"获取红利指数(000015)成分股失败，已达到最大重试次数")
    return [], None

def batch_download_stocks(stock_symbols, batch_num, total_batches, resume=False):
    """下载单个批次的数据，添加批次级交易日检查"""
    logger.info(f"开始下载批次 {batch_num}/{total_batches}...")
    all_data = []
    failed_stocks = []
    valid_stocks = 0
    skipped_stocks = 0
    new_records = 0
    up_to_date_stocks = 0
    
    # 计算本批次的股票范围（每批次固定160个股票）
    batch_size = 160
    start_idx = (batch_num - 1) * batch_size
    end_idx = min(batch_num * batch_size, len(stock_symbols))
    batch_symbols = stock_symbols[start_idx:end_idx]
    
    logger.info(f"批次 {batch_num}/{total_batches}: 下载股票 {start_idx+1}-{end_idx} (共{len(batch_symbols)}只)")
    
    # 检查整个批次是否有必要下载
    if batch_symbols:
        # 1. 批次级交易日检查 - 使用第一只股票作为代表
        first_symbol = batch_symbols[0]
        cache_file = get_cache_filename(first_symbol)
        last_date = get_last_date_in_cache(cache_file)
        
        # 如果有缓存数据
        if last_date:
            start_date = (pd.Timestamp(last_date) + timedelta(days=1)).strftime('%Y-%m-%d')
            if not has_trading_days(start_date, end_date_str):
                logger.info(f"批次 {batch_num}/{total_batches} 在 {start_date} 到 {end_date_str} 之间无交易日，跳过整个批次")
                print(f"批次 {batch_num}/{total_batches}: 无交易日，跳过整个批次")
                return {
                    "failed_stocks": [],
                    "valid_stocks": 0,
                    "skipped_stocks": len(batch_symbols),
                    "total_stocks": len(batch_symbols),
                    "new_records": 0,
                    "up_to_date_stocks": len(batch_symbols)
                }
    
    # 下载本批次的股票数据
    if batch_symbols:
        # 创建进度条
        progress_desc = f"下载股票数据 (批次 {batch_num}/{total_batches})"
        
        # 确保在正确显示
        with tqdm(total=len(batch_symbols), desc=progress_desc, unit="股票", leave=True) as progress_bar:
            for symbol in batch_symbols:
                try:
                    # 更新进度条描述
                    progress_bar.set_postfix_str(f"股票: {symbol} - 状态: 检查中")
                    
                    # 检查缓存文件以确定最后日期
                    cache_file = get_cache_filename(symbol)
                    last_date = get_last_date_in_cache(cache_file)
                    
                    # 如果数据已是最新，跳过下载
                    if last_date and last_date >= end_date_str:
                        progress_bar.set_postfix_str(f"股票: {symbol} - 状态: 已最新")
                        skipped_stocks += 1
                        up_to_date_stocks += 1
                        progress_bar.update(1)
                        continue
                    
                    # 检查是否有交易日
                    if last_date:
                        start_date = (pd.Timestamp(last_date) + timedelta(days=1)).strftime('%Y-%m-%d')
                        if not has_trading_days(start_date, end_date_str):
                            progress_bar.set_postfix_str(f"股票: {symbol} - 状态: 无交易日")
                            skipped_stocks += 1
                            progress_bar.update(1)
                            continue
                    
                    progress_bar.set_postfix_str(f"股票: {symbol} - 状态: 下载中")
                    
                    df = download_stock_data(symbol)
                    if not df.empty:
                        if len(df) >= MIN_HISTORY_DAYS:
                            all_data.append(df)
                            valid_stocks += 1
                            new_records += len(df)
                            progress_bar.set_postfix_str(f"股票: {symbol} - 状态: 成功 - 天数: {len(df)}")
                        else:
                            logger.warning(f"跳过数据不足的股票: {symbol} (仅{len(df)}天数据)")
                            failed_stocks.append(symbol)
                            skipped_stocks += 1
                            progress_bar.set_postfix_str(f"股票: {symbol} - 状态: 数据不足 - 天数: {len(df)}")
                    else:
                        # 如果下载返回空，可能是没有新交易日数据
                        skipped_stocks += 1
                        progress_bar.set_postfix_str(f"股票: {symbol} - 状态: 无新数据")
                    time.sleep(REQUEST_DELAY)
                except Exception as e:
                    logger.error(f"处理股票 {symbol} 时出错: {e}")
                    failed_stocks.append(symbol)
                    progress_bar.set_postfix_str(f"股票: {symbol} - 状态: 错误")
                    time.sleep(REQUEST_DELAY * 2)
                finally:
                    progress_bar.update(1)
    else:
        logger.warning(f"批次 {batch_num}/{total_batches} 没有需要下载的股票数据")
    
    # 保存本批次的结果
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df["date"] = pd.to_datetime(combined_df["date"])
        
        # 排序和去重
        combined_df = combined_df.sort_values(["symbol", "date"])
        combined_df = combined_df.drop_duplicates(subset=["symbol", "date"], keep="last")
        
        # 保存到临时文件
        batch_save_path = os.path.join(DATA_DIR, f"prices_batch_{batch_num}.csv")
        combined_df.to_csv(batch_save_path, index=False)
        logger.info(f"批次 {batch_num}/{total_batches} 价格数据已保存至: {batch_save_path}, 新增记录: {new_records}, 有效股票: {valid_stocks}只")
    else:
        logger.warning(f"批次 {batch_num}/{total_batches} 无有效数据可保存")
    
    # 返回本批次统计信息
    return {
        "failed_stocks": failed_stocks,
        "valid_stocks": valid_stocks,
        "skipped_stocks": skipped_stocks,
        "total_stocks": len(batch_symbols),
        "new_records": new_records,
        "up_to_date_stocks": up_to_date_stocks
    }

def merge_batch_files(total_batches):
    """合并所有批次文件，包括指数数据"""
    logger.info("开始合并所有批次文件...")
    # 只删除临时批次文件，不再合并生成大文件
    logger.info("已合并所有批次和指数数据到内存，但不再生成单一的prices.csv大文件。请直接使用分段prices_*.csv文件进行后续分析和读取。")
    for i in range(1, total_batches + 1):
        batch_file = os.path.join(DATA_DIR, f"prices_batch_{i}.csv")
        if os.path.exists(batch_file):
            try:
                os.remove(batch_file)
                logger.info(f"已删除临时文件: {os.path.basename(batch_file)}")
            except Exception as e:
                logger.error(f"删除临时文件失败: {e}")
    return True

def download_all_data(max_stocks=0, request_delay=REQUEST_DELAY, resume=False, total_batches=3):
    """下载所有需要的数据并保存，支持断点续传和分批下载"""
    # 检查上次下载日志
    last_download_info = load_download_log(DOWNLOAD_LOG_FILE)
    if last_download_info:
        last_end_time = last_download_info.get("download_end_time")
        if last_end_time:
            logger.info(f"上次下载完成时间: {last_end_time}")
    
    # 记录下载开始时间
    download_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"开始下载所有数据 (共分{total_batches}个批次)...")
    all_failed_stocks = []
    total_valid_stocks = 0
    total_skipped_stocks = 0
    total_stocks_processed = 0
    total_new_records = 0
    total_up_to_date_stocks = 0
    
    # 1. 处理待恢复状态
    pending_stocks = None
    if resume:
        pending_stocks = load_pending_stocks()
        failed_indexes, failed_stocks = load_failed_tasks()
        
        if failed_indexes or failed_stocks:
            logger.warning(f"发现之前失败的任务: {len(failed_indexes)}只指数, {len(failed_stocks)}只股票")
    
    # 2. 下载指数数据
    index_symbols = list(STYLE_INDEX_SYMBOLS.keys())
    
    # 如果有失败的指数，优先重试
    if resume:
        failed_indexes, _ = load_failed_tasks()
        if failed_indexes:
            index_symbols = failed_indexes + index_symbols
    
    # 下载指数
    index_results = {}
    logger.info(f"开始下载 {len(index_symbols)} 只指数数据...")
    
    # 为指数下载添加进度条
    with tqdm(total=len(index_symbols), desc="下载指数数据", unit="指数", leave=True) as pbar:
        for symbol in index_symbols:
            df = download_index_data(symbol)
            if not df.empty:
                index_results[symbol] = True
            else:
                index_results[symbol] = False
            time.sleep(request_delay)
            pbar.update(1)
    
    # 检查指数下载结果
    failed_indexes = [sym for sym, success in index_results.items() if not success]
    if failed_indexes:
        logger.error(f"以下指数下载失败: {', '.join(failed_indexes)}")
    
    # 3. 获取各指数成分股并合并
    if pending_stocks is not None:
        logger.info(f"使用待处理的股票列表 ({len(pending_stocks)}只股票)")
        stock_symbols = pending_stocks
        latest_date = None  # 恢复模式下不关心最新日期
    else:
        logger.info("获取各指数成分股...")
        all_stock_symbols = []
        success_count = 0
        
        # 遍历所有风格指数获取成分股
        for index_symbol in STYLE_INDEX_SYMBOLS.keys():
            stock_symbols, latest_date = get_constituents(index_symbol)
            
            if stock_symbols and latest_date:
                logger.info(f"获取到{STYLE_INDEX_SYMBOLS[index_symbol]}({index_symbol})最新成分股({latest_date.strftime('%Y-%m-%d')})，共{len(stock_symbols)}只股票")
                all_stock_symbols.extend(stock_symbols)
                success_count += 1
            else:
                logger.error(f"获取{STYLE_INDEX_SYMBOLS[index_symbol]}({index_symbol})成分股失败")
        
        # 合并去重所有成分股
        if all_stock_symbols:
            stock_symbols = list(set(all_stock_symbols))  # 去重
            logger.info(f"合并后共{len(stock_symbols)}只股票")
            
            # 清理非成分股数据
            clean_non_constituent_stocks(stock_symbols)
            
            # 保存待处理股票列表，用于可能的恢复
            save_pending_stocks(stock_symbols)
        else:
            stock_symbols = []
            logger.error("获取所有指数成分股均失败")

    # 4. 处理失败的股票（如果有）
    if resume:
        _, failed_stocks = load_failed_tasks()
        if failed_stocks:
            # 将失败的股票添加到下载列表前面
            stock_symbols = failed_stocks + stock_symbols
            logger.info(f"将之前失败的 {len(failed_stocks)} 只股票添加到下载列表")
    
    # 5. 限制下载股票数量
    if max_stocks > 0 and len(stock_symbols) > max_stocks:
        logger.info(f"限制下载前{max_stocks}只股票（实际成分股{len(stock_symbols)}只）")
        stock_symbols = stock_symbols[:max_stocks]
    
    # 6. 分批下载股票数据
    total_stocks = len(stock_symbols)
    if total_stocks == 0:
        logger.error("没有需要下载的股票数据")
        return False, failed_indexes, []
    
    # 加载批次状态（用于恢复）
    current_batch, saved_total_batches, completed_batches = load_batch_state()
    if resume and current_batch is not None and saved_total_batches == total_batches:
        start_batch = current_batch
        logger.info(f"从批次 {start_batch}/{total_batches} 恢复下载")
    else:
        start_batch = 1
        completed_batches = 0
        # 清除旧的批次状态
        clear_batch_state()
    
    # 分批下载
    for batch_num in range(start_batch, total_batches + 1):
        logger.info(f"开始处理批次 {batch_num}/{total_batches}...")
        save_batch_state(batch_num, total_batches, completed_batches)
        
        # 下载本批次数据
        batch_result = batch_download_stocks(
            stock_symbols,
            batch_num, 
            total_batches,
            resume
        )
        
        # 记录本批次结果
        all_failed_stocks.extend(batch_result["failed_stocks"])
        total_valid_stocks += batch_result["valid_stocks"]
        total_skipped_stocks += batch_result["skipped_stocks"]
        total_stocks_processed += batch_result["total_stocks"]
        total_new_records += batch_result["new_records"]
        total_up_to_date_stocks += batch_result["up_to_date_stocks"]
        
        # 更新状态
        completed_batches += 1
        save_batch_state(batch_num + 1, total_batches, completed_batches)
        
        # 打印本批次摘要
        batch_summary = (
            f"批次 {batch_num}/{total_batches} 完成: "
            f"成功 {batch_result['valid_stocks']}, "
            f"跳过 {batch_result['skipped_stocks']} (其中已最新: {batch_result['up_to_date_stocks']}), "
            f"失败 {len(batch_result['failed_stocks'])}, "
            f"新增记录 {batch_result['new_records']}"
        )
        logger.info(batch_summary)
        print(batch_summary)
        
        # 如果不是最后一个批次，暂停指定时间
        if batch_num < total_batches:
            wait_minutes = BATCH_WAIT_MINUTES
            wait_seconds = wait_minutes * 60
            logger.info(f"批次 {batch_num}/{total_batches} 完成，等待{wait_minutes}分钟继续下一批次...")
            
            # 创建更友好的倒计时显示
            print(f"\n{'='*50}")
            print(f"批次 {batch_num}/{total_batches} 已完成，下一批次将在 {wait_minutes} 分钟后开始")
            print("您可以暂时离开，程序会自动继续")
            print("="*50)
            
            # 创建倒计时进度条
            with tqdm(total=wait_seconds, desc="等待中", unit="秒", leave=True) as pbar:
                for elapsed in range(0, wait_seconds, 10):
                    # 计算剩余时间
                    remaining = wait_seconds - elapsed
                    mins, secs = divmod(remaining, 60)
                    countdown_str = f"{mins:02d}:{secs:02d}"
                    
                    # 更新进度条描述
                    pbar.set_postfix_str(f"剩余时间: {countdown_str}")
                    
                    # 等待10秒并更新进度条
                    time.sleep(10)
                    pbar.update(10)
            
            print("\n" + "="*50)
            print(f"继续处理批次 {batch_num+1}/{total_batches}...")
            print("="*50 + "\n")
    
    # 7. 合并所有批次文件
    merge_success = merge_batch_files(total_batches)
    
    # 打印总体统计信息
    total_summary = (
        f"\n{'='*50}\n"
        f"所有批次完成! 统计信息:\n"
        f"总处理股票: {total_stocks_processed}\n"
        f"成功下载: {total_valid_stocks}\n"
        f"跳过: {total_skipped_stocks} (其中已最新: {total_up_to_date_stocks})\n"
        f"失败: {len(all_failed_stocks)}\n"
        f"新增记录总数: {total_new_records}\n"
        f"{'='*50}"
    )
    logger.info(total_summary)
    print(total_summary)
    
    # 8. 更新待处理股票列表
    if all_failed_stocks:
        logger.warning(f"本次有 {len(all_failed_stocks)} 只股票下载失败")
        save_pending_stocks(all_failed_stocks)
    elif os.path.exists(PENDING_STOCKS_FILE):
        try:
            os.remove(PENDING_STOCKS_FILE)
            logger.info("所有股票下载成功，已清除待处理列表")
        except Exception as e:
            logger.error(f"清除待处理列表时出错: {e}")
    
    # 9. 保存成分股列表
    if stock_symbols and not all_failed_stocks:
        constituents_df = pd.DataFrame({"symbol": stock_symbols})
        constituents_path = os.path.join(DATA_DIR, "constituents.csv")
        constituents_df.to_csv(constituents_path, index=False)
        logger.info(f"成分股列表已保存至: {constituents_path}")
    
    # 10. 清除失败记录（如果全部成功）
    if not failed_indexes and not all_failed_stocks:
        clear_failure_files()
        # 清除待处理列表
        if os.path.exists(PENDING_STOCKS_FILE):
            try:
                os.remove(PENDING_STOCKS_FILE)
            except:
                pass
    
    # 清除批次状态
    clear_batch_state()
    
    # 保存下载完成信息
    download_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    download_info = {
        "download_start_time": download_start_time,
        "download_end_time": download_end_time,
        "total_batches": total_batches,
        "total_stocks_processed": total_stocks_processed,
        "total_valid_stocks": total_valid_stocks,
        "total_skipped_stocks": total_skipped_stocks,
        "total_new_records": total_new_records,
        "failed_stocks_count": len(all_failed_stocks),
        "failed_indexes_count": len(failed_indexes)
    }
    save_download_log(DOWNLOAD_LOG_FILE, download_info)
    
    return merge_success, failed_indexes, all_failed_stocks

# 执行函数
def run_data_fetcher(resume=False, max_stocks=0, total_batches=3):
    """
    执行数据下载任务
    
    参数:
    resume: 是否恢复之前失败的任务 (默认False)
    max_stocks: 最大下载股票数量 (0表示无限制)
    total_batches: 总批次数 (默认3)
    """
    
    # 非恢复模式时清除旧状态
    if not resume:
        clear_failure_files()
        clear_batch_state()
        if os.path.exists(PENDING_STOCKS_FILE):
            try:
                os.remove(PENDING_STOCKS_FILE)
            except:
                pass
    
    try:
        start_time = time.time()
        print(f"{'='*50}")
        print(f"{'恢复模式' if resume else '全新模式'}启动数据下载 (共{total_batches}个批次)...")
        print(f"{'='*50}")
        logger.info(f"{'恢复模式' if resume else '全新模式'}启动数据下载 (共{total_batches}个批次)...")
        
        success, failed_indexes, failed_stocks = download_all_data(
            max_stocks=max_stocks, 
            resume=resume,
            total_batches=total_batches
        )
        
        elapsed = time.time() - start_time
        elapsed_str = str(timedelta(seconds=int(elapsed)))
        logger.info(f"任务完成! 总耗时: {elapsed:.2f}秒")
        
        # 创建用户友好的结果报告
        if success and not failed_indexes and not failed_stocks:
            status = "成功"
        elif success or failed_indexes or failed_stocks:
            status = "部分成功"
        else:
            status = "失败"
            
        result = {
            "status": status,
            "elapsed_time": f"{elapsed:.2f}秒",
            "failed_indexes": failed_indexes,
            "failed_stocks": failed_stocks,
            "failed_index_count": len(failed_indexes),
            "failed_stock_count": len(failed_stocks),
            "total_batches": total_batches
        }
        
        # 打印结果摘要
        logger.info(f"任务完成! 总耗时: {elapsed:.2f}秒")
        logger.info(f"状态: {status}")
        logger.info(f"总批次数: {total_batches}")
        logger.info(f"失败指数: {len(failed_indexes)} 只")
        logger.info(f"失败股票: {len(failed_stocks)} 只")
        
        if failed_indexes:
            logger.info(f"失败指数列表: {', '.join(failed_indexes)}")
        if failed_stocks:
            logger.info(f"失败股票数量: {len(failed_stocks)} 只")
        
        return result
            
    except Exception as e:
        logger.exception(f"未处理异常: {e}")
        return {"status": "错误", "exception": str(e)}

# ==================== 数据源类 ====================
# 已移除CSVDataSource类，使用data_source.py中的CustomDataSource类

# 数据加载测试函数
def test_data_loading():
    """测试自定义数据源的数据加载功能"""
    logger.info("="*60)
    logger.info("开始测试数据加载功能")
    logger.info("="*60)
    
    # 1. 检查数据文件是否存在
    prices_path = os.path.join(DATA_DIR, "prices.csv")
    constituents_path = os.path.join(DATA_DIR, "constituents.csv")
    
    if not os.path.exists(prices_path) or not os.path.exists(constituents_path):
        logger.error("数据文件不存在，请先运行数据下载脚本")
        return
    
    # 2. 创建自定义数据源实例
    logger.info("创建自定义数据源实例...")
    from data_source import CustomDataSource
    custom_data_source = CustomDataSource()
    logger.info("数据源创建成功")
    
    # 3. 获取所有成分股
    logger.info("获取成分股列表...")
    all_symbols = custom_data_source.get_all_symbols()
    logger.info(f"获取到 {len(all_symbols)} 只成分股")
    
    if len(all_symbols) > 0:
        logger.info(f"前5只成分股: {all_symbols[:5]}")
    else:
        logger.warning("未获取到成分股列表")
        # 尝试从价格文件中提取股票代码
        try:
            prices_df = pd.read_csv(prices_path, dtype={'symbol': str})
            prices_df['symbol'] = prices_df['symbol'].apply(lambda x: str(x).zfill(6))
            all_symbols = prices_df['symbol'].unique().tolist()
            logger.info(f"从价格文件中提取到 {len(all_symbols)} 只股票代码")
        except Exception as e:
            logger.error(f"无法从价格文件提取股票代码: {e}")
            return
    
    # 4. 选择测试股票
    if len(all_symbols) >= 5:
        test_symbols = all_symbols[:5]
        logger.info(f"选择测试股票: {test_symbols}")
    else:
        test_symbols = all_symbols
        logger.info(f"选择所有可用股票进行测试: {test_symbols}")
    
    # 5. 加载测试数据
    try:
        logger.info("开始加载测试数据...")
        start_date = "2023-01-01"
        end_date = "2023-12-31"
        test_data = custom_data_source.query(test_symbols, start_date, end_date)
        
        if not test_data.empty:
            logger.info(f"成功加载测试数据: {len(test_data)} 条记录")
            logger.info(f"数据日期范围: {test_data['date'].min()} 至 {test_data['date'].max()}")
            logger.info(f"涉及股票数量: {test_data['symbol'].nunique()}")
            
            # 显示每只股票的数据量
            symbol_counts = test_data.groupby('symbol').size()
            logger.info("各股票数据量:")
            for symbol, count in symbol_counts.items():
                logger.info(f"  {symbol}: {count} 条记录")
        else:
            logger.warning("测试数据为空")
            
    except Exception as e:
        logger.error(f"加载测试数据时出错: {e}")
        return
    
    # 6. 完成测试
    logger.info("="*60)
    logger.info("数据加载测试完成")
    logger.info("="*60)

# 创建示例数据函数（如果没有真实数据）
def create_sample_data():
    """创建示例数据用于测试"""
    logger.info("创建示例数据...")
    
    # 创建数据目录
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # 创建示例成分股
    symbols = ['000001', '600000', '000002', '600519', '000651']
    pd.DataFrame({'symbol': symbols}).to_csv(os.path.join(DATA_DIR, "constituents.csv"), index=False)
    logger.info(f"创建成分股文件: {len(symbols)}只股票")
    
    # 创建示例价格数据
    all_data = []
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2023, 1, 1)
    
    for symbol in symbols:
        dates = pd.date_range(start_date, end_date, freq='B')  # 工作日
        n = len(dates)
        
        # 生成随机价格数据
        base_price = np.random.uniform(10, 100)
        price_series = base_price + np.cumsum(np.random.normal(0, 1, n))
        
        opens = price_series + np.random.uniform(-0.5, 0.5, n)
        highs = opens + np.random.uniform(0.5, 2.0, n)
        lows = opens - np.random.uniform(0.5, 2.0, n)
        closes = (highs + lows) / 2
        volumes = np.random.randint(10000, 1000000, n)
        
        df = pd.DataFrame({
            'date': dates,
            'symbol': symbol,
            'open': np.round(opens, 2),
            'high': np.round(highs, 2),
            'low': np.round(lows, 2),
            'close': np.round(closes, 2),
            'volume': volumes
        })
        
        all_data.append(df)
    
    # 合并并保存
    pd.concat(all_data).to_csv(os.path.join(DATA_DIR, "prices.csv"), index=False)
    logger.info(f"创建价格数据文件: {len(all_data)}只股票的价格数据")

import sys
import os
import logging

logger = logging.getLogger(__name__)

# DATA_DIR已经在文件顶部定义过，不需要重复定义


if __name__ == "__main__":
    # 检查是否提供了命令行参数
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            # 检查数据文件是否存在
            prices_path = os.path.join(DATA_DIR, "prices.csv")
            constituents_path = os.path.join(DATA_DIR, "constituents.csv")
            
            if not os.path.exists(prices_path) or not os.path.exists(constituents_path):
                logger.warning("数据文件不存在，创建示例数据...")
                create_sample_data()
            
            # 执行测试
            test_data_loading()
            
        elif sys.argv[1] == "download":
            # 完整下载模式
            print("开始全新下载模式...")
            result = run_data_fetcher(resume=False, max_stocks=0, total_batches=3)
            if result.get("status") != "取消" and result.get("status") != "中断":
                print("\n最终执行结果:", result)
            else:
                print("下载操作已取消")
        elif sys.argv[1] == "resume":
            # 断点续传模式
            print("开始断点续传模式下载...")
            result = run_data_fetcher(resume=True, max_stocks=0, total_batches=3)
            if result.get("status") != "取消" and result.get("status") != "中断":
                print("\n最终执行结果:", result)
            else:
                print("下载操作已取消")
        else:
            print("无效参数。使用方法:")
            print("  python data_fetch.py test     - 运行测试")
            print("  python data_fetch.py download - 全新下载模式")
            print("  python data_fetch.py resume   - 断点续传模式")
    else:
        # 默认运行数据获取程序
        run_data_fetcher()
        sys.exit(0)

