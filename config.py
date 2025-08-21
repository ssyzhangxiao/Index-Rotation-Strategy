#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# config.py
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass


@dataclass
class BacktestConfig:
    """回测配置类"""
    data_path: Path = Path('./data')
    result_file: str = 'selected_stocks.csv'
    # 将回测周期调整为最近3年
    end_date: str = datetime.today().strftime('%Y-%m-%d')
    start_date: str = (datetime.today() - timedelta(days=1095)).strftime('%Y-%m-%d')  # 3年 = 1095天
    hold_days: int = 5
    initial_capital: float = 1000000.0
    trade_fee: float = 0.0003  # 万三手续费
    stamp_tax: float = 0.001   # 千一印花税（仅卖出）
    slippage: float = 0.001    # 0.1%滑点
    slippage_min: float = 0.0005  # 最小滑点
    slippage_max: float = 0.0015  # 最大滑点
    stop_loss: float = 0.08    # 8%止损线
    max_position_size: float = 0.05  # 每只股票最大仓位5%
    
    # 数据下载配置
    data_dir: str = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    constituents_cache_dir: str = os.path.join(data_dir, "constituents_cache")
    price_cache_dir: str = os.path.join(data_dir, "price_cache")
    
    # 数据下载日期范围
    data_download_start_date: str = "2010-01-01"
    data_download_end_date: str = datetime.now().strftime("%Y-%m-%d")
    
    # 股票筛选条件
    filter_st: bool = True  # 是否过滤ST股票
    filter_paused: bool = True  # 是否过滤停牌股票
    filter_limit_up: bool = True  # 是否过滤涨停股票
    filter_limit_down: bool = True  # 是否过滤跌停股票
    
    # 下载参数
    max_retries: int = 5
    request_delay: int = 1
    min_history_days_download: int = 100  # 数据下载专用
    batch_wait_minutes: int = 10
    
    # 策略参数
    top_n: int = 15
    lookback: int = 5
    min_history_days_backtest: int = 5  # 回测专用
    min_stock_count: int = 10  # 添加缺少的属性，最少选股数量
    
    # 技术指标参数
    # RSI参数
    rsi_period: int = 6
    
    # 威廉指标参数
    williams_r_period_1: int = 21      # 威廉指标第一个周期
    williams_r_period_2: int = 42      # 威廉指标第二个周期
    williams_r_overbought: float = -80   # 超买线
    williams_r_oversold: float = -20     # 超卖线
    
    # 均线参数
    ma_short_period: int = 10          # 短期均线周期
    ma_long_period: int = 20           # 长期均线周期
    
    # 选股策略参数
    strong_index_count: int = 3        # 强势指数数量
    rising_pct_threshold: float = 0.5  # 10日涨幅大于50%视为短期涨幅过大
    ma_down_threshold: float = 0.98    # 均线向下排列的阈值

# 创建全局配置实例
CONFIG = BacktestConfig()


# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# 指数配置
STYLE_INDEX_SYMBOLS = {
    "000015": "红利指数",
    "399374": "中盘成长",
    "399324": "深证红利",
    "399376": "小盘成长",
    "399006": "创业板指",
    "399372": "大盘成长",
    "399321": "国证红利"
}

# 主要指数列表（用于选股）
INDEX_LIST = ['399372', '399374', '399376', '399006', '399324', '399321', '000015']

# 失败记录文件（基于CONFIG中的路径配置）
FAILED_INDEX_FILE = os.path.join(CONFIG.data_dir, "failed_indexes.txt")
FAILED_STOCKS_FILE = os.path.join(CONFIG.data_dir, "failed_stocks.txt")
PENDING_STOCKS_FILE = os.path.join(CONFIG.data_dir, "pending_stocks.txt")
BATCH_STATE_FILE = os.path.join(CONFIG.data_dir, "batch_state.txt")

# 记录配置信息
logger.info(f"回测期间: {CONFIG.start_date} 至 {CONFIG.end_date}")
logger.info(f"初始资金: {CONFIG.initial_capital}")
logger.info(f"每次持有股票数量: {CONFIG.top_n}")
logger.info(f"收益率计算周期: {CONFIG.lookback}天")
logger.info(f"最小历史数据天数: {CONFIG.min_history_days_backtest}")