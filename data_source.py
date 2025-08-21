#!/usr/bin/env python3
# coding: utf-8

# In[5]:


import os
import pandas as pd
import pybroker as pb
from pybroker import ExecContext
from datetime import datetime, timedelta
import logging
# 使用正确的导入路径 - pybroker包应该通过pybroker.data导入DataSource
from pybroker.data import DataSource

# 配置日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 导入统一配置
from config import CONFIG, STYLE_INDEX_SYMBOLS

# 本地定义所有配置信息，使用config.py的配置
DATA_DIR = CONFIG.data_dir
CONSTITUENTS_CACHE_DIR = CONFIG.constituents_cache_dir
PRICE_CACHE_DIR = CONFIG.price_cache_dir

# 使用统一的指数配置
STYLE_INDEX_SYMBOLS = STYLE_INDEX_SYMBOLS


class CustomDataSource(DataSource):
    """自定义数据源，从CSV文件加载数据"""
    def __init__(self, data_dir=DATA_DIR):
        super().__init__()
        self.data_dir = data_dir

    def _fetch_data(self, symbols, start_date, end_date, timeframe, adjust):
        """
        实现DataSource的抽象方法，从 price_cache 目加载所有单文件数据
        """
        from data_fetch import read_all_price_cache_df
        df = read_all_price_cache_df()
        
        if df.empty:
            logger.error("未找到任何价格数据文件")
            return pd.DataFrame()
            
        # 处理列名差异 - 确保有symbol列
        if 'symbol' not in df.columns:
            if '股票代码' in df.columns:
                df['symbol'] = df['股票代码']
            elif '指数代码' in df.columns:
                df['symbol'] = df['指数代码']
            else:
                logger.warning("警告: 数据中既没有'symbol'列也没有'股票代码'列或'指数代码'列")
                return pd.DataFrame()
                
        df['symbol'] = df['symbol'].astype(str)
        df['symbol'] = df['symbol'].str.replace(r'\.(SH|SZ)$', '', regex=True)
        
        # 过滤请求的symbols
        if symbols:
            symbols = [str(s).replace('.SH', '').replace('.SZ', '') for s in symbols]
            index_symbols = [s for s in symbols if s.startswith('000') or s.startswith('399')]
            stock_symbols = [s for s in symbols if not (s.startswith('000') or s.startswith('399'))]
            
            if index_symbols and '指数代码' in df.columns:
                # 对于指数数据，使用指数代码列进行过滤
                df_index = df[df['指数代码'].isin(index_symbols)]
                # 对于股票数据，使用symbol列进行过滤
                df_stocks = df[df['symbol'].isin(stock_symbols)]
                # 合并两种数据
                df = pd.concat([df_index, df_stocks], ignore_index=True)
            else:
                df = df[df['symbol'].isin(symbols)]
                
        # 转换日期参数为datetime对象
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # 日期过滤
        df['date'] = pd.to_datetime(df['date'])
        df_filtered = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
        
        # 如果按精确日期范围过滤后没有数据，尝试放宽条件
        if df_filtered.empty:
            # 查找最接近请求日期范围的数据
            min_date = df['date'].min()
            max_date = df['date'].max()
            logger.info(f"精确日期范围内无数据，数据实际范围: {min_date} 至 {max_date}")
            logger.info(f"请求日期范围: {start_date} 至 {end_date}")
            
            # 如果请求的结束日期早于数据最早日期或请求的开始日期晚于数据最晚日期，则确实无数据
            if end_date < min_date or start_date > max_date:
                logger.warning("请求日期范围与实际数据日期范围无重叠")
                return pd.DataFrame()
            
            # 否则使用实际可用的数据范围
            actual_start = max(start_date, min_date)
            actual_end = min(end_date, max_date)
            logger.info(f"调整日期范围为: {actual_start} 至 {actual_end}")
            df_filtered = df[(df['date'] >= actual_start) & (df['date'] <= actual_end)]
            
            if df_filtered.empty:
                logger.warning("即使调整日期范围后仍然无数据")
                return pd.DataFrame()
        
        required_columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
        for col in required_columns:
            if col not in df_filtered.columns:
                # 尝试从其他可能的列名映射
                column_mapping = {
                    'open': ['open', '开盘价'],
                    'high': ['high', '最高价'],
                    'low': ['low', '最低价'],
                    'close': ['close', '收盘价'],
                    'volume': ['volume', '成交量']
                }
                
                found = False
                for target_col, possible_names in column_mapping.items():
                    for name in possible_names:
                        if name in df_filtered.columns:
                            df_filtered.rename(columns={name: target_col}, inplace=True)
                            found = True
                            break
                    if found:
                        break
                
                if not found and col not in df_filtered.columns:
                    raise ValueError(f"Missing required column: {col}")
                
        # 选择并重新排列列
        available_columns = [col for col in required_columns if col in df_filtered.columns]
        df_filtered = df_filtered[available_columns]
        df_filtered['roc'] = df_filtered.groupby('symbol')['close'].pct_change(periods=10) * 100
        
        logger.info(f"成功获取数据，共{len(df_filtered)}条记录，包含{df_filtered['symbol'].nunique()}个标的")
        return df_filtered

    def get_stock_industry(self, symbol):
        """
        获取股票的行业信息
        
        Args:
            symbol (str): 股票代码
            
        Returns:
            str: 股票所属行业
        """
        try:
            # 导入data_fetch模块中的函数
            from data_fetch import get_stock_industry
            return get_stock_industry(symbol)
        except Exception as e:
            logger.warning(f"获取股票{symbol}行业信息失败: {e}")
            return "未知"

    def get_all_symbols(self):
        """
        获取所有可用的股票代码
        
        Returns:
            list: 所有股票代码列表
        """
        try:
            # 首先尝试从constituents_cache目录中的所有成分股文件获取股票
            if os.path.exists(CONSTITUENTS_CACHE_DIR):
                all_symbols = set()
                for filename in os.listdir(CONSTITUENTS_CACHE_DIR):
                    if filename.startswith("constituents_") and filename.endswith(".csv"):
                        filepath = os.path.join(CONSTITUENTS_CACHE_DIR, filename)
                        try:
                            df = pd.read_csv(filepath, dtype={'成分股代码': str})
                            if not df.empty and '成分股代码' in df.columns:
                                symbols = df['成分股代码'].tolist()
                                all_symbols.update(symbols)
                        except Exception as e:
                            logger.warning(f"读取{filename}时出错: {e}")
                
                if all_symbols:
                    symbols = list(all_symbols)
                    logger.info(f"从constituents_cache目录获取到{len(symbols)}只股票")
                    return symbols
            
            # 如果没有constituents_cache目录或文件，则从prices.csv中提取
            prices_path = os.path.join(self.data_dir, "prices.csv")
            if os.path.exists(prices_path):
                df = pd.read_csv(prices_path, dtype={'symbol': str})
                if not df.empty and 'symbol' in df.columns:
                    # 获取所有唯一的股票代码
                    symbols = df['symbol'].unique().tolist()
                    # 修复逻辑：只有在STYLE_INDEX_SYMBOLS中的才被认为是指数
                    from config import STYLE_INDEX_SYMBOLS
                    symbols = [symbol for symbol in symbols 
                              if symbol not in STYLE_INDEX_SYMBOLS]
                    
                    # 添加后缀：上海交易所股票加.SH，深圳交易所股票加.SZ
                    formatted_symbols = []
                    for symbol in symbols:
                        if symbol.endswith('.SH') or symbol.endswith('.SZ'):
                            formatted_symbols.append(symbol)  # 已有后缀，直接添加
                        elif symbol.startswith('6') or symbol.startswith('5') or symbol.startswith('9'):
                            formatted_symbols.append(f"{symbol}.SH")
                        elif symbol.startswith('0') or symbol.startswith('3') or symbol.startswith('2'):
                            formatted_symbols.append(f"{symbol}.SZ")
                        else:
                            formatted_symbols.append(symbol)  # 保留原有格式
                    
                    logger.info(f"从prices.csv获取到{len(formatted_symbols)}只股票")
                    return formatted_symbols
            
            logger.warning("无法获取股票代码列表")
            return []
        except Exception as e:
            logger.error(f"获取所有股票代码时出错: {e}")
            return []

    def get_index_constituents(self, index_code):
        """
        获取指定指数的成分股列表，兼容分文件方案。
        Args:
            index_code (str): 指数代码
        Returns:
            list: 成分股代码列表
        """
        try:
            # 优先从 constituents_cache 目录读取
            if os.path.exists(CONSTITUENTS_CACHE_DIR):
                for filename in os.listdir(CONSTITUENTS_CACHE_DIR):
                    if filename == f"constituents_{index_code}.csv":
                        filepath = os.path.join(CONSTITUENTS_CACHE_DIR, filename)
                        df = pd.read_csv(filepath, dtype={'成分股代码': str})
                        if not df.empty and '成分股代码' in df.columns:
                            return df['成分股代码'].tolist()
            # 如果没有缓存文件，则尝试从分文件数据中推断
            from data_fetch import read_all_price_cache_df
            df = read_all_price_cache_df()
            if 'index_code' in df.columns:
                stocks = df[df['index_code'] == index_code]['symbol'].unique().tolist()
                if stocks:
                    return stocks
            # 兜底返回空列表
            return []
        except Exception as e:
            logger.error(f"获取指数{index_code}成分股失败: {e}")
            return []

    def validate_strategy_data(self):
        """
        校验策略运行所需的基础数据是否可用。
        Returns:
            bool: 数据可用返回True，否则False
        """
        try:
            # 检查分文件目录和至少有一个csv文件
            if not os.path.exists(PRICE_CACHE_DIR):
                logger.error("价格缓存目录不存在")
                return False
            files = [f for f in os.listdir(PRICE_CACHE_DIR) if f.endswith('.csv')]
            if not files:
                logger.error("价格缓存目录下没有任何csv文件")
                return False
            # 尝试读取部分数据
            from data_fetch import read_all_price_cache_df
            df = read_all_price_cache_df()
            if df is None or df.empty:
                logger.error("分文件数据读取为空")
                return False
            required_columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
            for col in required_columns:
                if col not in df.columns:
                    logger.error(f"缺少必要字段: {col}")
                    return False
            return True
        except Exception as e:
            logger.error(f"数据校验异常: {e}")
            return False

    def load_test_data(self, days=5):
        """
        加载测试数据以验证数据源可用性（兼容分文件）
        """
        try:
            logger.info(f"加载{days}天测试数据以验证数据源...")
            from data_fetch import read_all_price_cache_df
            df = read_all_price_cache_df()
            if df.empty:
                logger.warning("无法获取测试数据")
                return None
            end_date = datetime.today()
            start_date = end_date - timedelta(days=days)
            df = df[(df['date'] >= pd.to_datetime(start_date)) & (df['date'] <= pd.to_datetime(end_date))]
            logger.info(f"成功加载测试数据，共{len(df)}条记录")
            return df
        except Exception as e:
            logger.error(f"加载测试数据失败: {str(e)}")
            return None
    
    def load_index_data(self, index_code, days=365):
        """
        加载指数数据
        
        Args:
            index_code (str): 指数代码
            days (int): 加载天数，默认365天
            
        Returns:
            pandas.DataFrame: 指数数据
        """
        try:
            logger.info(f"加载指数{index_code}数据...")
            end_date = datetime.today()
            start_date = end_date - timedelta(days=days)
            
            # 尝试直接查询指数数据
            index_data = self.query(
                [index_code],
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                timeframe='1d',
                adjust=''
            )
            
            # 如果直接查询失败，尝试使用带后缀的指数代码
            if index_data is None or index_data.empty:
                # 尝试添加.SH或.SZ后缀
                suffixed_index_code = None
                if index_code.startswith('000'):
                    suffixed_index_code = f"{index_code}.SH"
                elif index_code.startswith('399'):
                    suffixed_index_code = f"{index_code}.SZ"
                
                if suffixed_index_code:
                    logger.info(f"尝试使用带后缀的指数代码: {suffixed_index_code}")
                    index_data = self.query(
                        [suffixed_index_code],
                        start_date=start_date.strftime('%Y-%m-%d'),
                        end_date=end_date.strftime('%Y-%m-%d'),
                        timeframe='1d',
                        adjust=''
                    )
            
            if index_data is None or index_data.empty:
                logger.warning(f"无法获取指数{index_code}数据")
                return None
            
            logger.info(f"成功加载指数{index_code}数据，共{len(index_data)}条记录")
            return index_data
        except Exception as e:
            logger.error(f"加载指数{index_code}数据失败: {str(e)}")
            return None
    
    def load_stock_data(self, symbols, days=90):
        """
        加载股票数据
        
        Args:
            symbols (list): 股票代码列表
            days (int): 加载天数，默认90天
            
        Returns:
            pandas.DataFrame: 股票数据
        """
        try:
            logger.info(f"加载{len(symbols)}只股票数据...")
            end_date = datetime.today()
            start_date = end_date - timedelta(days=days)
            
            stock_data = self.query(
                symbols,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                timeframe='1d',
                adjust=''
            )
            
            if stock_data is None or stock_data.empty:
                logger.warning("无法获取股票数据")
                return None
            
            logger.info(f"成功加载股票数据，共{len(stock_data)}条记录")
            return stock_data
        except Exception as e:
            logger.error(f"加载股票数据失败: {str(e)}")
            return None
    
    def load_all_symbols(self, days=30):
        """
        加载所有股票代码
        
        Args:
            days (int): 加载天数，默认30天
            
        Returns:
            list: 股票代码列表
        """
        try:
            logger.info("加载所有股票代码...")
            end_date = datetime.today()
            start_date = end_date - timedelta(days=days)
            
            all_data = self.query(
                [],  # 获取所有股票数据
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                timeframe='1d',
                adjust=''
            )
            
            if all_data is None or all_data.empty:
                logger.warning("无法获取所有股票数据")
                return []
            
            symbols = list(all_data['symbol'].unique())
            logger.info(f"成功加载所有股票代码，共{len(symbols)}只股票")
            return symbols
        except Exception as e:
            logger.error(f"加载所有股票代码失败: {str(e)}")
            return None
    
    def query(self, symbols, start_date, end_date, timeframe='1d', adjust=''):
        """
        查询数据
        :param symbols: 股票代码列表
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param timeframe: 时间框架 (PyBroker传递的参数)
        :param adjust: 调整参数 (PyBroker传递的参数)
        :return: DataFrame
        """
        logger.info(f"开始读取数据文件...")
        
        try:
            # 使用分文件方案读取所有数据
            from data_fetch import read_all_price_cache_df
            df = read_all_price_cache_df()
            logger.info(f"成功读取分文件数据，共 {len(df)} 行数据")
            
            if df.empty:
                logger.warning("分文件数据为空")
                return df
            
            # 确保日期列是datetime类型
            df['date'] = pd.to_datetime(df['date'])
            
            # 日期过滤
            original_start = df['date'].min()
            original_end = df['date'].max()
            logger.info(f"原始数据日期范围: {original_start} 至 {original_end}")
            
            # 处理列名差异 - 确保有symbol列
            if 'symbol' in df.columns:
                # 已经有symbol列，无需处理
                pass
            elif '股票代码' in df.columns:
                # 将"股票代码"列重命名为"symbol"
                df['symbol'] = df['股票代码']
            elif '指数代码' in df.columns:
                # 将"指数代码"列重命名为"symbol"
                df['symbol'] = df['指数代码']
            else:
                logger.warning("警告: 数据中既没有'symbol'列也没有'股票代码'列或'指数代码'列")
                return pd.DataFrame()
            
            # 确保symbol列为字符串类型并处理数值型代码
            df['symbol'] = df['symbol'].astype(str)
            # 处理NaN值和小数点
            df['symbol'] = df['symbol'].apply(lambda x: x.split('.')[0] if '.' in x and x != 'nan' else x)
            df = df[df['symbol'] != 'nan']
            
            # 统一符号格式，去除任何后缀
            df['symbol'] = df['symbol'].str.replace(r'\.(SH|SZ)$', '', regex=True)
            
            # 处理指数代码特殊需求：保留原始格式的指数代码
            if '指数代码' in df.columns:
                # 处理指数代码列中的数值格式
                df['指数代码'] = df['指数代码'].astype(str).apply(lambda x: x.split('.')[0] if '.' in x and x != 'nan' else x)
                df['指数代码'] = df['指数代码'].apply(lambda x: x.zfill(6) if x.isdigit() else x)  # 补齐前导零
                df['symbol'] = df.apply(lambda row: row['指数代码'] if pd.notna(row['指数代码']) and row['指数代码'] != 'nan' else row['symbol'], axis=1)
            
            # 过滤请求的symbols
            if symbols:
                # 统一请求符号格式，去除任何后缀，确保保留前导零
                processed_symbols = []
                for s in symbols:
                    # 转换为字符串并去除可能的后缀
                    str_s = str(s).replace('.SH', '').replace('.SZ', '')
                    # 确保指数代码格式正确（特别是以0开头的代码）
                    if str_s.isdigit() and len(str_s) <= 6:
                        # 补齐前导零
                        str_s = str_s.zfill(6)
                    processed_symbols.append(str_s)
                
                symbols = processed_symbols
                
                # 区分指数代码和股票代码
                # 修复逻辑：只有6位数字且在STYLE_INDEX_SYMBOLS中的才被认为是指数
                from config import STYLE_INDEX_SYMBOLS
                index_symbols = [s for s in symbols if s in STYLE_INDEX_SYMBOLS]
                stock_symbols = [s for s in symbols if s not in STYLE_INDEX_SYMBOLS]
                
                logger.info(f"请求的指数代码: {index_symbols}")
                logger.info(f"请求的股票代码: {stock_symbols}")
                
                # 特别检查000015指数
                if "000015" in index_symbols:
                    logger.info("请求列表中包含000015指数")
                
                # 过滤数据
                if index_symbols and '指数代码' in df.columns:
                    # 对于指数数据，使用指数代码列进行过滤
                    # 确保指数代码也补齐前导零
                    formatted_index_symbols = [s.zfill(6) if s.isdigit() and len(s) < 6 else s for s in index_symbols]
                    df_index = df[df['指数代码'].isin(formatted_index_symbols)]
                    # 对于股票数据，使用symbol列进行过滤
                    df_stocks = df[df['symbol'].isin(stock_symbols)]
                    # 合并两种数据
                    df = pd.concat([df_index, df_stocks], ignore_index=True)
                else:
                    df = df[df['symbol'].isin(symbols)]
            
            start_date = pd.to_datetime(start_date)
            end_date = pd.to_datetime(end_date)
            
            # 日期过滤
            df_filtered = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
            
            # 如果按精确日期范围过滤后没有数据，尝试放宽条件
            if df_filtered.empty:
                # 查找最接近请求日期范围的数据
                min_date = df['date'].min() if not df.empty else None
                max_date = df['date'].max() if not df.empty else None
                
                if min_date is not None and max_date is not None:
                    logger.info(f"精确日期范围内无数据，数据实际范围: {min_date} 至 {max_date}")
                    logger.info(f"请求日期范围: {start_date} 至 {end_date}")
                    
                    # 如果请求的结束日期早于数据最早日期或请求的开始日期晚于数据最晚日期，则确实无数据
                    if end_date < min_date or start_date > max_date:
                        logger.warning("请求日期范围与实际数据日期范围无重叠")
                    else:
                        # 否则使用实际可用的数据范围
                        actual_start = max(start_date, min_date)
                        actual_end = min(end_date, max_date)
                        logger.info(f"调整日期范围为: {actual_start} 至 {actual_end}")
                        df_filtered = df[(df['date'] >= actual_start) & (df['date'] <= actual_end)]
            
            logger.info(f"根据日期范围 {start_date.date()} 至 {end_date.date()} 过滤后剩余 {len(df_filtered)} 行数据")
            
            if df_filtered.empty:
                logger.warning("警告: 根据日期范围过滤后无数据")
                return df_filtered
            
            # 确保列的顺序和类型正确
            required_columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
            for col in required_columns:
                if col not in df_filtered.columns:
                    # 尝试从其他可能的列名映射
                    column_mapping = {
                        'open': ['open', '开盘价', 'Open'],
                        'high': ['high', '最高价', 'High'],
                        'low': ['low', '最低价', 'Low'],
                        'close': ['close', '收盘价', 'Close'],
                        'volume': ['volume', '成交量', 'Volume']
                    }
                    
                    found = False
                    for target_col, possible_names in column_mapping.items():
                        if col == target_col:
                            for name in possible_names:
                                if name in df_filtered.columns:
                                    df_filtered.rename(columns={name: target_col}, inplace=True)
                                    found = True
                                    break
                            break
                    
                    if not found and col not in df_filtered.columns:
                        raise ValueError(f"Missing required column: {col}")
            
            # 选择并排序列
            available_columns = [col for col in required_columns if col in df_filtered.columns]
            df_filtered = df_filtered[available_columns]
            
            # 计算动量指标ROC
            df_filtered['roc'] = df_filtered.groupby('symbol')['close'].pct_change(periods=10) * 100
            
            logger.info(f"最终返回数据: {len(df_filtered)} 行, {df_filtered['symbol'].nunique()} 只股票")
            
            return df_filtered
        
        except Exception as e:
            logger.error(f"读取数据文件时出错: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()

    def _select_date_range(self):
        """交互式选择日期范围"""
        logger.info("\n" + "="*50)
        logger.info("请选择数据加载的时间范围:")
        logger.info("="*50)
        logger.info("1. 过去30天")
        logger.info("2. 过去90天")
        logger.info("3. 过去180天")
        logger.info("4. 过去1年")
        logger.info("5. 过去3年")
        logger.info("6. 自定义日期范围")
        logger.info("7. 全部数据")
        logger.info("-"*50)
        
        while True:
            try:
                choice = input("请输入选项编号 (1-7, 默认为3): ").strip()
                if not choice:
                    choice = "3"  # 默认选择180天
                
                choice = int(choice)
                if 1 <= choice <= 7:
                    break
                else:
                    logger.warning("请输入1-7之间的数字")
            except ValueError:
                logger.warning("请输入有效的数字")
        
        end_date = datetime.now()
        
        if choice == 1:
            start_date = end_date - timedelta(days=30)
        elif choice == 2:
            start_date = end_date - timedelta(days=90)
        elif choice == 3:
            start_date = end_date - timedelta(days=180)
        elif choice == 4:
            start_date = end_date - timedelta(days=365)
        elif choice == 5:
            start_date = end_date - timedelta(days=365*3)
        elif choice == 6:
            # 自定义日期范围
            while True:
                try:
                    start_str = input("请输入开始日期 (YYYY-MM-DD): ").strip()
                    start_date = datetime.strptime(start_str, "%Y-%m-%d")
                    break
                except ValueError:
                    logger.warning("日期格式错误，请使用 YYYY-MM-DD 格式")
            
            while True:
                try:
                    end_str = input("请输入结束日期 (YYYY-MM-DD): ").strip()
                    end_date = datetime.strptime(end_str, "%Y-%m-%d")
                    if end_date < start_date:
                        logger.warning("结束日期不能早于开始日期")
                        continue
                    break
                except ValueError:
                    logger.warning("日期格式错误，请使用 YYYY-MM-DD 格式")
        else:  # choice == 7
            # 全部数据，使用一个较早的开始日期
            start_date = datetime(2010, 1, 1)
        
        logger.info(f"选定的时间范围: {start_date.date()} 至 {end_date.date()}")
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")