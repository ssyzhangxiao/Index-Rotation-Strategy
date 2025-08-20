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
        super().__init__()  # 调用父类初始化
        self.data_dir = data_dir
        self.data_cache = {}
        
        # 确保目录存在
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(CONSTITUENTS_CACHE_DIR, exist_ok=True)
        os.makedirs(PRICE_CACHE_DIR, exist_ok=True)
        
        # 注册需要的列
        pb.register_columns('roc')  # 注册动量指标列
        
    def validate_data_files(self):
        """
        检查必要的数据文件是否存在
        
        Returns:
            bool: 数据文件完整性检查结果
        """
        logger.info("=== 数据文件完整性检查 ===")
        
        # 检查数据目录
        if not os.path.exists(self.data_dir):
            logger.error(f"错误: 数据目录 {self.data_dir} 不存在")
            return False
            
        # 检查价格数据文件
        prices_file = os.path.join(self.data_dir, "prices.csv")
        if not os.path.exists(prices_file):
            logger.error(f"错误: 价格数据文件 {prices_file} 不存在")
            return False
            
        logger.info(f"✓ 价格数据文件存在: {prices_file}")
        
        # 检查成分股文件
        constituents_file = os.path.join(self.data_dir, "constituents.csv")
        if os.path.exists(constituents_file):
            logger.info(f"✓ 成分股文件存在: {constituents_file}")
        else:
            logger.warning(f"⚠ 成分股文件不存在: {constituents_file}")
            
        return True

    def get_trade_days(self, start_date, end_date):
        """
        获取指定日期范围内的交易日列表
        
        Args:
            start_date (str): 开始日期，格式'YYYY-MM-DD'
            end_date (str): 结束日期，格式'YYYY-MM-DD'
            
        Returns:
            list: 交易日列表
        """
        # 从CSV文件中获取数据以确定实际的交易日
        prices_path = os.path.join(self.data_dir, "prices.csv")
        if not os.path.exists(prices_path):
            raise FileNotFoundError(f"价格数据文件不存在: {prices_path}")
        
        # 读取数据并获取日期范围内的交易日
        df = pd.read_csv(prices_path, parse_dates=['date'])
        df = df[(df['date'] >= pd.to_datetime(start_date)) & 
                (df['date'] <= pd.to_datetime(end_date))]
        
        # 获取唯一的交易日并排序
        trade_days = sorted(df['date'].unique().tolist())
        return trade_days

    def validate_price_data(self):
        """
        验证价格数据的完整性和质量
        
        Returns:
            bool: 价格数据验证结果
        """
        logger.info("=== 价格数据验证 ===")
        
        prices_file = os.path.join(self.data_dir, 'prices.csv')
        if not os.path.exists(prices_file):
            logger.warning(f"警告: 价格数据文件 {prices_file} 不存在，跳过验证")
            return True
        
        try:
            # 读取价格数据
            logger.info("正在读取价格数据...")
            df = pd.read_csv(prices_file, low_memory=False)
            logger.info(f"✓ 成功读取价格数据，共 {len(df)} 行")
            
            # 检查必要列
            required_columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"错误: 缺少必要列: {missing_columns}")
                return False
            else:
                logger.info("✓ 所有必需列都存在")
            
            # 检查日期范围
            try:
                df['date'] = pd.to_datetime(df['date'])
                date_range = df['date'].min(), df['date'].max()
                logger.info(f"✓ 日期范围: {date_range[0].date()} 至 {date_range[1].date()}")
            except Exception as e:
                logger.warning(f"警告: 日期列转换失败: {e}")
            
            # 检查symbol列
            unique_symbols = df['symbol'].nunique()
            logger.info(f"✓ 唯一股票代码数量: {unique_symbols}")
            
            # 检查空值
            null_counts = df.isnull().sum()
            null_columns = null_counts[null_counts > 0]
            if len(null_columns) > 0:
                logger.warning("警告: 以下列存在空值:")
                for col, count in null_columns.items():
                    logger.warning(f"  {col}: {count} 个空值")
            else:
                logger.info("✓ 数据中没有空值")
                
            return True
            
        except Exception as e:
            logger.error(f"验证价格数据时出错: {e}")
            return False

    def validate_constituents_data(self):
        """
        验证成分股数据的完整性和质量
        
        Returns:
            bool: 成分股数据验证结果
        """
        logger.info("=== 成分股数据验证 ===")
        
        if not os.path.exists(CONSTITUENTS_CACHE_DIR):
            logger.warning(f"警告: 成分股缓存目录 {CONSTITUENTS_CACHE_DIR} 不存在，跳过验证")
            return True
        
        try:
            constituents_files = [f for f in os.listdir(CONSTITUENTS_CACHE_DIR) if f.startswith('constituents_') and f.endswith('.csv')]
            logger.info(f"找到 {len(constituents_files)} 个成分股文件")
            
            total_stocks = 0
            for file in constituents_files:
                try:
                    file_path = os.path.join(CONSTITUENTS_CACHE_DIR, file)
                    df = pd.read_csv(file_path)
                    stock_count = len(df)
                    total_stocks += stock_count
                    logger.info(f"  {file}: {stock_count} 只股票")
                except Exception as e:
                    logger.error(f"  读取 {file} 时出错: {e}")
            
            logger.info(f"✓ 成分股数据总览: {len(constituents_files)} 个指数，共 {total_stocks} 只股票")
            return True
            
        except Exception as e:
            logger.error(f"验证成分股数据时出错: {e}")
            return False

    def validate_strategy_data(self):
        """
        验证策略执行所需的数据
        
        Returns:
            bool: 策略数据验证结果
        """
        logger.info("=== 策略数据验证 ===")
        
        # 检查数据文件
        if not self.validate_data_files():
            return False
        
        # 验证价格数据
        if not self.validate_price_data():
            return False
        
        # 验证成分股数据
        if not self.validate_constituents_data():
            return False
        
        logger.info("✓ 所有数据验证通过，可以执行策略")
        return True
    def get_index_constituents(self, index_code):
        """
        获取指数成分股列表
        
        Args:
            index_code (str): 指数代码
            
        Returns:
            list: 成分股代码列表
        """
        try:
            # 首先尝试从特定指数的缓存文件获取成分股
            index_constituents_path = os.path.join(CONSTITUENTS_CACHE_DIR, f"constituents_{index_code}.csv")
            if os.path.exists(index_constituents_path):
                df = pd.read_csv(index_constituents_path, dtype={'成分股代码': str})
                if not df.empty and '成分股代码' in df.columns:
                    constituents = df['成分股代码'].tolist()
                    logger.info(f"从{os.path.basename(index_constituents_path)}获取到指数{index_code}的成分股{len(constituents)}只")
                    return constituents
            
            # 如果没有特定指数文件，则尝试从constituents.csv文件获取成分股
            constituents_path = os.path.join(self.data_dir, "constituents.csv")
            if os.path.exists(constituents_path):
                df = pd.read_csv(constituents_path, dtype={'symbol': str})
                if not df.empty and 'symbol' in df.columns:
                    # 如果有index_code列，则根据index_code筛选
                    if 'index_code' in df.columns:
                        constituents = df[df['index_code'] == index_code]['symbol'].tolist()
                        logger.info(f"从constituents.csv获取到指数{index_code}的成分股{len(constituents)}只")
                        return constituents
                    else:
                        # 否则尝试从prices.csv中提取特定指数的成分股
                        logger.warning(f"constituents.csv文件中没有index_code列，尝试从prices.csv推断成分股")
            
            # 从prices.csv中提取特定指数成分股
            prices_path = os.path.join(self.data_dir, "prices.csv")
            if os.path.exists(prices_path):
                df = pd.read_csv(prices_path, dtype={'symbol': str}, parse_dates=['date'])
                if not df.empty and 'symbol' in df.columns:
                    # 获取最近一段时间的数据
                    recent_date = df['date'].max() - timedelta(days=30)
                    recent_df = df[df['date'] >= recent_date]
                    # 筛选属于该指数的成分股
                    index_stocks = recent_df[recent_df['symbol'].str.contains(str(index_code), na=False)]['symbol'].unique().tolist()
                    if index_stocks:
                        logger.info(f"从prices.csv推断出指数{index_code}的成分股{len(index_stocks)}只")
                        return index_stocks
                    
                    # 如果没找到直接匹配的股票，则获取出现频率最高的股票作为成分股
                    symbol_counts = recent_df['symbol'].value_counts()
                    # 过滤掉明显是指数的代码
                    constituents = [symbol for symbol in symbol_counts.index 
                                  if not ((symbol.startswith('399') or symbol.startswith('000')) 
                                          and len(symbol) == 6 and symbol.isdigit())][:500]  # 限制最多500只
                    logger.info(f"从prices.csv推断出成分股{len(constituents)}只")
                    return constituents[:200]  # 限制最多200只
            
            logger.warning(f"无法获取指数{index_code}的成分股列表")
            return []
        except Exception as e:
            logger.error(f"获取指数{index_code}成分股时出错: {e}")
            return []

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

    def load_test_data(self, days=5):
        """
        加载测试数据以验证数据源可用性
        
        Args:
            days (int): 加载天数，默认5天
            
        Returns:
            pandas.DataFrame: 测试数据
        """
        try:
            logger.info(f"加载{days}天测试数据以验证数据源...")
            end_date = datetime.today()
            start_date = end_date - timedelta(days=days)
            
            test_data = self.query(
                [],  # 获取所有股票数据
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                timeframe='1d',
                adjust=''
            )
            
            if test_data is None or test_data.empty:
                logger.warning("无法获取测试数据")
                return None
            
            logger.info(f"成功加载测试数据，共{len(test_data)}条记录")
            return test_data
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
    
    def _fetch_data(self, symbols, start_date, end_date, timeframe, adjust):
        """从CSV文件加载数据"""
        file_path = os.path.join(self.data_dir, 'prices.csv')
        if not os.path.exists(file_path):
            # 如果当前目录没有prices.csv，尝试使用offline_data目录
            offline_data_path = "./offline_data"
            file_path = os.path.join(offline_data_path, 'prices.csv')
            if not os.path.exists(file_path):
                logger.error(f"数据文件 {os.path.join(self.data_dir, 'prices.csv')} 和 {file_path} 都不存在")
                return pd.DataFrame()
            else:
                logger.info(f"使用离线数据文件: {file_path}")
        
        try:
            df = pd.read_csv(file_path, parse_dates=['date'])
            logger.info(f"成功读取数据文件，共 {len(df)} 行数据")
        except Exception as e:
            logger.error(f"读取数据文件失败: {e}")
            return pd.DataFrame()
        
        if df.empty:
            logger.warning("警告: 数据文件为空")
            return df
            
        # 显示原始数据信息
        logger.info(f"原始数据日期范围: {df['date'].min()} 至 {df['date'].max()}")
        if 'symbol' in df.columns:
            logger.info(f"原始数据股票数量: {df['symbol'].nunique()}")
        elif '股票代码' in df.columns:
            logger.info(f"原始数据股票数量: {df['股票代码'].nunique()}")
        else:
            logger.warning("警告: 数据中既没有'symbol'列也没有'股票代码'列")
        
        # 处理列名差异 - 确保有symbol列
        if 'symbol' in df.columns:
            # 已经有symbol列，无需处理
            pass
        elif '股票代码' in df.columns:
            # 将"股票代码"列重命名为"symbol"
            df['symbol'] = df['股票代码']
        else:
            logger.warning("警告: 数据中既没有'symbol'列也没有'股票代码'列")
            return pd.DataFrame()
        
        # 确保symbol列为字符串类型
        df['symbol'] = df['symbol'].astype(str)
        
        # 统一符号格式，去除任何后缀
        df['symbol'] = df['symbol'].str.replace(r'\.(SH|SZ)$', '', regex=True)
        
        # 处理指数代码特殊需求：保留原始格式的指数代码
        if 'index_code' in df.columns:
            df['symbol'] = df.apply(lambda row: row['index_code'] if pd.notna(row['index_code']) else row['symbol'], axis=1)
        
        # 过滤请求的symbols
        if symbols:
            # 统一请求符号格式，去除任何后缀
            symbols = [str(s).replace('.SH', '').replace('.SZ', '') for s in symbols]
            
            # 区分指数代码和股票代码
            index_symbols = [s for s in symbols if s.startswith('000') or s.startswith('399')]
            stock_symbols = [s for s in symbols if not (s.startswith('000') or s.startswith('399'))]
            
            # 对于指数代码，尝试匹配symbol列或index_code列
            if index_symbols and 'index_code' in df.columns:
                original_len = len(df)
                df = df[df['index_code'].isin(index_symbols) | df['symbol'].isin(stock_symbols)]
                logger.info(f"指数代码过滤: {len(df)} 行 (原: {original_len} 行)")
            else:
                original_symbol_count = df['symbol'].nunique()
                df = df[df['symbol'].isin(symbols)]
                logger.info(f"根据symbols过滤后剩余 {len(df)} 行数据 (股票数量: {df['symbol'].nunique()}/{original_symbol_count})")
        
        # 转换日期参数为datetime对象
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # 检查结束日期是否是周末，如果是则调整到最近的交易日
        if end_date.weekday() == 5:  # Saturday
            end_date = end_date - timedelta(days=1)
            logger.info(f"结束日期是周六，调整到最近的交易日: {end_date.date()}")
        elif end_date.weekday() == 6:  # Sunday
            end_date = end_date - timedelta(days=2)
            logger.info(f"结束日期是周日，调整到最近的交易日: {end_date.date()}")
        
        # 检查请求的日期是否超出实际数据范围
        actual_end_date = df['date'].max()
        if end_date > actual_end_date:
            logger.info(f"请求的结束日期 {end_date.date()} 超出实际数据范围，调整为实际最新日期: {actual_end_date.date()}")
            end_date = actual_end_date
            
        actual_start_date = df['date'].min()
        if start_date < actual_start_date:
            logger.info(f"请求的开始日期 {start_date.date()} 超出实际数据范围，调整为实际最早日期: {actual_start_date.date()}")
            start_date = actual_start_date

        # 检查开始日期是否是周末，如果是则调整到最近的交易日
        if start_date.weekday() == 5:  # Saturday
            start_date = start_date - timedelta(days=1)
            logger.info(f"开始日期是周六，调整到最近的交易日: {start_date.date()}")
        elif start_date.weekday() == 6:  # Sunday
            start_date = start_date - timedelta(days=2)
            logger.info(f"开始日期是周日，调整到最近的交易日: {start_date.date()}")
        
        # 确保数据中的日期列是datetime类型
        df['date'] = pd.to_datetime(df['date'])
        
        # 过滤日期范围
        original_count = len(df)
        logger.info(f"过滤前数据量: {original_count}")
        logger.info(f"请求的日期范围: {start_date} 至 {end_date}")
        logger.info(f"实际数据日期范围: {df['date'].min()} 至 {df['date'].max()}")
        
        # 检查是否有日期重叠
        if df['date'].max() < start_date or df['date'].min() > end_date:
            logger.warning("警告: 请求的日期范围与实际数据日期范围无重叠")
        else:
            logger.info("日期范围有重叠，进行过滤")
        
        df_filtered = df[(df['date'] >= start_date) & 
                        (df['date'] <= end_date)]
        
        logger.info(f"根据日期范围 {start_date.date()} 至 {end_date.date()} 过滤后剩余 {len(df_filtered)} 行数据")
        
        if df_filtered.empty:
            logger.warning("警告: 根据日期范围过滤后无数据")
            # 显示请求的日期范围和实际数据日期范围
            logger.info(f"请求的日期范围: {start_date.date()} 至 {end_date.date()}")
            if original_count > 0:
                logger.info(f"文件中的日期范围: {df['date'].min().date() if len(df) > 0 else 'N/A'} 至 {df['date'].max().date() if len(df) > 0 else 'N/A'}")
                sample_dates = df['date'].head().tolist() if len(df) > 0 else []
                logger.info(f"过滤前数据中的示例日期: {sample_dates}")
            else:
                # 显示原始数据的日期范围
                try:
                    original_df = pd.read_csv(file_path, parse_dates=['date'])
                    if not original_df.empty:
                        logger.info(f"文件中的日期范围: {original_df['date'].min().date()} 至 {original_df['date'].max().date()}")
                        sample_dates = original_df['date'].head().tolist()
                        logger.info(f"数据中的示例日期: {sample_dates}")
                except Exception as e:
                    logger.error(f"读取原始数据出错: {e}")
            return df_filtered
        
        # 确保列的顺序和类型正确
        required_columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
        for col in required_columns:
            if col not in df_filtered.columns:
                raise ValueError(f"Missing required column: {col}")
        
        # 选择并排序列
        df_filtered = df_filtered[required_columns]
        
        # 计算动量指标ROC
        df_filtered['roc'] = df_filtered.groupby('symbol')['close'].pct_change(periods=10) * 100
        
        logger.info(f"最终返回数据: {len(df_filtered)} 行, {df_filtered['symbol'].nunique()} 只股票")
        return df_filtered

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
            # 读取CSV文件
            file_path = os.path.join(self.data_dir, 'prices.csv')
            df = pd.read_csv(file_path, parse_dates=['date'])
            logger.info(f"成功读取数据文件，共 {len(df)} 行数据")
            
            if df.empty:
                logger.warning("数据文件为空")
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
            else:
                logger.warning("警告: 数据中既没有'symbol'列也没有'股票代码'列")
                return pd.DataFrame()
            
            # 确保symbol列为字符串类型
            df['symbol'] = df['symbol'].astype(str)
            
            # 统一符号格式，去除任何后缀
            df['symbol'] = df['symbol'].str.replace(r'\.(SH|SZ)$', '', regex=True)
            
            # 处理指数代码特殊需求：保留原始格式的指数代码
            if 'index_code' in df.columns:
                df['symbol'] = df.apply(lambda row: row['index_code'] if pd.notna(row['index_code']) else row['symbol'], axis=1)
            
            # 过滤请求的symbols
            if symbols:
                # 统一请求符号格式，去除任何后缀，确保保留前导零
                processed_symbols = []
                for s in symbols:
                    # 转换为字符串并去除可能的后缀
                    str_s = str(s).replace('.SH', '').replace('.SZ', '')
                    # 确保指数代码格式正确（特别是以0开头的代码）
                    if str_s.isdigit() and len(str_s) == 6:
                        # 保留前导零
                        processed_symbols.append(str_s)
                    else:
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
                
                # 收集过滤后的数据
                filtered_dfs = []
                
                # 处理指数数据
                if index_symbols:
                    df_index_filtered = df[df['symbol'].isin(index_symbols)]
                    filtered_dfs.append(df_index_filtered)
                    logger.info(f"通过symbol列匹配到 {len(df_index_filtered)} 行指数数据")
                    # 特别检查000015指数数据
                    if "000015" in index_symbols:
                        df_000015 = df[df['symbol'] == "000015"]
                        logger.info(f"通过symbol列匹配到000015指数 {len(df_000015)} 行数据")
                
                # 处理股票数据
                if stock_symbols:
                    df_stock_filtered = df[df['symbol'].isin(stock_symbols)]
                    filtered_dfs.append(df_stock_filtered)
                    logger.info(f"匹配到 {len(df_stock_filtered)} 行股票数据")
                
                # 合并所有过滤后的数据
                if filtered_dfs:
                    df = pd.concat(filtered_dfs, ignore_index=True)
                    logger.info(f"合并后的数据总行数: {len(df)}")
                else:
                    df = pd.DataFrame()  # 如果没有匹配到任何数据，返回空DataFrame
                    logger.info("没有匹配到任何数据")
                
                logger.info(f"指数和股票代码过滤完成，剩余 {len(df)} 行数据")
            else:
                logger.info("未指定特定股票代码，返回所有数据")
            
            # 转换日期参数为datetime对象
            start_date = pd.to_datetime(start_date)
            end_date = pd.to_datetime(end_date)
            
            # 检查结束日期是否是周末，如果是则调整到最近的交易日
            if end_date.weekday() == 5:  # Saturday
                end_date = end_date - timedelta(days=1)
                logger.info(f"结束日期是周六，调整到最近的交易日: {end_date.date()}")
            elif end_date.weekday() == 6:  # Sunday
                end_date = end_date - timedelta(days=2)
                logger.info(f"结束日期是周日，调整到最近的交易日: {end_date.date()}")
            
            # 检查请求的日期是否超出实际数据范围
            actual_end_date = df['date'].max()
            if end_date > actual_end_date:
                logger.info(f"请求的结束日期 {end_date.date()} 超出实际数据范围，调整为实际最新日期: {actual_end_date.date()}")
                end_date = actual_end_date
            
            actual_start_date = df['date'].min()
            if start_date < actual_start_date:
                logger.info(f"请求的开始日期 {start_date.date()} 超出实际数据范围，调整为实际最早日期: {actual_start_date.date()}")
                start_date = actual_start_date

            # 检查开始日期是否是周末，如果是则调整到最近的交易日
            if start_date.weekday() == 5:  # Saturday
                start_date = start_date - timedelta(days=1)
                logger.info(f"开始日期是周六，调整到最近的交易日: {start_date.date()}")
            elif start_date.weekday() == 6:  # Sunday
                start_date = start_date - timedelta(days=2)
                logger.info(f"开始日期是周日，调整到最近的交易日: {start_date.date()}")
            
            # 确保数据中的日期列是datetime类型
            df['date'] = pd.to_datetime(df['date'])
            
            # 过滤日期范围
            original_count = len(df)
            logger.info(f"过滤前数据量: {original_count}")
            logger.info(f"请求的日期范围: {start_date} 至 {end_date}")
            logger.info(f"实际数据日期范围: {df['date'].min()} 至 {df['date'].max()}")
            
            # 检查是否有日期重叠
            if df['date'].max() < start_date or df['date'].min() > end_date:
                logger.warning("警告: 请求的日期范围与实际数据日期范围无重叠")
            else:
                logger.info("日期范围有重叠，进行过滤")
            
            df_filtered = df[(df['date'] >= start_date) & 
                            (df['date'] <= end_date)]
            
            logger.info(f"根据日期范围 {start_date.date()} 至 {end_date.date()} 过滤后剩余 {len(df_filtered)} 行数据")
            
            if df_filtered.empty:
                logger.warning("警告: 根据日期范围过滤后无数据")
                # 显示请求的日期范围和实际数据日期范围
                logger.info(f"请求的日期范围: {start_date.date()} 至 {end_date.date()}")
                if original_count > 0:
                    logger.info(f"文件中的日期范围: {df['date'].min().date() if len(df) > 0 else 'N/A'} 至 {df['date'].max().date() if len(df) > 0 else 'N/A'}")
                    sample_dates = df['date'].head().tolist() if len(df) > 0 else []
                    logger.info(f"过滤前数据中的示例日期: {sample_dates}")
                else:
                    # 显示原始数据的日期范围
                    try:
                        original_df = pd.read_csv(file_path, parse_dates=['date'])
                        if not original_df.empty:
                            logger.info(f"文件中的日期范围: {original_df['date'].min().date()} 至 {original_df['date'].max().date()}")
                            sample_dates = original_df['date'].head().tolist()
                            logger.info(f"数据中的示例日期: {sample_dates}")
                    except Exception as e:
                        logger.error(f"读取原始数据出错: {e}")
                return df_filtered
            
            # 确保列的顺序和类型正确
            required_columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
            for col in required_columns:
                if col not in df_filtered.columns:
                    raise ValueError(f"Missing required column: {col}")
            
            # 选择并排序列
            df_filtered = df_filtered[required_columns]
            
            # 计算动量指标ROC
            df_filtered['roc'] = df_filtered.groupby('symbol')['close'].pct_change(periods=10) * 100
            
            logger.info(f"最终返回数据: {len(df_filtered)} 行, {df_filtered['symbol'].nunique()} 只股票")
            
            return df_filtered
        
        except Exception as e:
            logger.error(f"读取数据文件时出错: {e}")
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