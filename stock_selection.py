import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import traceback

# 导入统一配置
from config import CONFIG, INDEX_LIST

# 导入统一日志配置
from logger import setup_logger

# 确保日志目录存在
log_dir = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(log_dir, exist_ok=True)

# 创建日志文件路径
log_file_path = os.path.join(log_dir, 'stock_selection.log')

# 使用统一的日志配置
logger = setup_logger(log_file_path)

# 参数配置（已移至config.py，此处仅保留引用）
DATA_PATH = CONFIG.data_dir
STRONG_INDEX_COUNT = CONFIG.strong_index_count
SELECT_COUNT = CONFIG.top_n
RSI_PERIOD = CONFIG.rsi_period
RISING_PCT_THRESHOLD = CONFIG.rising_pct_threshold
MA_DOWN_THRESHOLD = CONFIG.ma_down_threshold



# 使用CustomDataSource类
from data_source import CustomDataSource
data_source = CustomDataSource()

def get_top_n_strong_indexes(index_list=None, n=3, selection_date=None):
    """
    基于RSI5指标获取短期强势指数
    
    :param index_list: 指数列表
    :param n: 返回前n个强势指数
    :param selection_date: 选股日期
    :return: 指数代码列表
    """
    logger.info("正在获取短期强势指数...")
    
    # 如果没有提供selection_date，则使用今天日期
    if selection_date is None:
        selection_date = datetime.today().strftime('%Y-%m-%d')
        logger.info(f"未提供选股日期，使用当前日期: {selection_date}")
    
    # 获取指数的历史数据并计算技术指标强度
    if index_list is not None and len(index_list) > 0:
        logger.info(f"基于选股日期计算短期强势指数: {selection_date}")
        # 计算开始日期（获取过去15天的数据以确保有足够数据计算5日RSI）
        end_date_obj = datetime.strptime(selection_date, '%Y-%m-%d')
        start_date = end_date_obj - timedelta(days=15)  # 获取约15天的数据
        
        logger.info(f"请求指数数据日期范围: {start_date.strftime('%Y-%m-%d')} 至 {selection_date}")
        logger.info(f"请求的指数列表: {index_list}")
        
        # 特别检查000015指数是否在请求列表中
        if "000015" in index_list:
            logger.info("请求列表中包含000015指数")
        
        try:
            # 查询指数数据
            index_data = data_source.query(
                symbols=index_list,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=selection_date
            )
            
            logger.info(f"从数据源获取到的指数数据: {len(index_data)} 条记录")
            if not index_data.empty:
                logger.info(f"获取到的指数数量: {index_data['symbol'].nunique()}")
                unique_symbols = list(index_data['symbol'].unique())
                logger.info(f"获取到的指数代码: {unique_symbols}")
                
                # 特别检查000015指数数据
                index_000015_data = index_data[index_data['symbol'] == "000015"]
                logger.info(f"000015指数的数据条数: {len(index_000015_data)}")
                if len(index_000015_data) > 0:
                    logger.info(f"000015指数的日期范围: {index_000015_data['date'].min()} 至 {index_000015_data['date'].max()}")
                
                # 检查每个请求的指数是否有数据
                for index in index_list:
                    index_specific_data = index_data[index_data['symbol'] == index]
                    logger.info(f"指数 {index} 的数据条数: {len(index_specific_data)}")
                    if len(index_specific_data) > 0:
                        logger.info(f"指数 {index} 的最新数据日期: {index_specific_data['date'].max()}")
                    else:
                        logger.warning(f"指数 {index} 没有匹配到任何数据")
            else:
                logger.warning("未能获取到任何指数数据")
                # 显示请求参数以便调试
                logger.info(f"请求参数 - 指数列表: {index_list}")
                logger.info(f"请求参数 - 开始日期: {start_date.strftime('%Y-%m-%d')}")
                logger.info(f"请求参数 - 结束日期: {selection_date}")
                
                # 特别检查数据文件中是否包含000015数据
                try:
                    prices_file = os.path.join(DATA_PATH, "prices.csv")
                    if os.path.exists(prices_file):
                        # 检查文件中是否有000015数据
                        import subprocess
                        result = subprocess.run(["grep", "-c", "000015", prices_file], 
                                              capture_output=True, text=True)
                        if result.returncode == 0:
                            count = int(result.stdout.strip())
                            logger.info(f"prices.csv文件中000015数据条数: {count}")
                        else:
                            logger.warning("无法统计prices.csv文件中000015数据条数")
                except Exception as e:
                    logger.error(f"检查prices.csv中000015数据时出错: {e}")
            
            if not index_data.empty:
                # 计算每个指数的RSI5强度
                index_rsi5_values = {}
                
                for symbol in index_list:
                    symbol_data = index_data[index_data['symbol'] == symbol].copy()
                    logger.info(f"指数 {symbol} 的数据条数: {len(symbol_data)}")
                    if len(symbol_data) >= 5:  # 确保有足够的数据计算RSI5
                        # 获取最近5日收盘价
                        recent_prices = symbol_data['close'].tail(5).tolist()
                        logger.info(f"指数 {symbol} 最近5日收盘价: {recent_prices}")
                        
                        # 计算RSI5
                        rsi5 = calc_rsi5(recent_prices)
                        index_rsi5_values[symbol] = rsi5
                        
                        logger.info(f"指数 {symbol} 的RSI5值: {rsi5}")
                    else:
                        # 数据不足，给默认评分0
                        index_rsi5_values[symbol] = 0
                        logger.warning(f"指数 {symbol} 数据不足，仅 {len(symbol_data)} 条记录")
                
                # 按RSI5值排序并返回前n个
                sorted_indexes = sorted(index_rsi5_values.items(), key=lambda x: x[1], reverse=True)
                top_indexes = [index for index, _ in sorted_indexes[:n]]
                logger.info(f"基于RSI5计算的强势指数: {top_indexes}")
                logger.info(f"各指数RSI5值: {sorted_indexes[:n]}")
                return top_indexes
            else:
                logger.warning("无法获取指数数据")
        except Exception as e:
            logger.error(f"计算指数强度时出错: {str(e)}")
            logger.exception(e)
    else:
        logger.warning(f"指数列表为空或无效: index_list={index_list}")
    
    # 如果没有提供selection_date或计算失败，返回空列表
    logger.warning("无法计算强势指数，返回空列表")
    return []

def calc_rsi5(prices):
    """
    计算5日RSI指标
    
    :param prices: 最近5日收盘价列表
    :return: RSI5值
    """
    deltas = np.diff(prices)
    gains = deltas[deltas > 0].mean() if len(deltas[deltas > 0]) > 0 else 0
    losses = -deltas[deltas < 0].mean() if len(deltas[deltas < 0]) > 0 else 1e-10  # 避免除零
    
    rs = gains / losses if losses != 0 else 0
    rsi = 100 - 100 / (1 + rs)
    return rsi

def generate_action_suggestions_report(index_rsi_values, index_action_suggestions):
    """
    生成操作建议报告
    
    :param index_rsi_values: 指数RSI值字典
    :param index_action_suggestions: 指数操作建议字典
    """
    # 按RSI值排序
    sorted_indices = sorted(index_rsi_values.items(), key=lambda x: x[1], reverse=True)
    
    logger.info("=== 指数操作建议报告 ===")
    logger.info("指数代码\tRSI5\t\t操作建议")
    logger.info("-" * 40)
    for index, rsi_value in sorted_indices:
        action = index_action_suggestions.get(index, "未知")
        logger.info(f"{index}\t\t{rsi_value:.1f}\t\t{action}")
    
    # 止损机制说明
    logger.info("\n=== 止损机制 ===")
    logger.info("持仓指数RSI5跌破55：减半仓")
    logger.info("RSI5跌破45：清仓")

def calculate_market_volatility(index_data):
    """
    计算市场波动率
    
    :param index_data: 指数数据
    :return: 市场波动率
    """
    try:
        # 计算所有指数的20日收益率
        recent_data = index_data.groupby('symbol').tail(20)
        returns = recent_data.groupby('symbol')['close'].apply(
            lambda x: x.pct_change().dropna()
        )
        
        # 计算平均波动率
        if len(returns) > 0:
            avg_volatility = np.std(returns)
            return avg_volatility
        else:
            return 0.0
    except Exception as e:
        logger.warning(f"计算市场波动率时出错: {e}")
        return 0.0

def handle_special_market_conditions(index_rsi_values):
    """
    处理特殊市场情况
    
    :param index_rsi_values: 指数RSI值字典
    """
    if not index_rsi_values:
        return
    
    rsi_values = list(index_rsi_values.values())
    avg_rsi = np.mean(rsi_values)
    rsi_std = np.std(rsi_values)
    
    logger.info(f"市场状态分析 - 平均RSI: {avg_rsi:.2f}, RSI标准差: {rsi_std:.2f}")
    
    # 全市场超买
    if avg_rsi > 80:
        logger.warning("全市场超买状态 (RSI > 80)，建议降低总仓位至50%，增配防御性板块")
    
    # 全市场超卖
    if avg_rsi < 40:
        logger.warning("全市场超卖状态 (RSI < 40)，建议保持30%底仓，开启定投模式")
    
    # 指数分化
    if rsi_std > 15:
        logger.warning("市场分化严重 (RSI标准差 > 15)，建议加大轮动强度，调仓周期缩短至3天")

def check_data_availability():
    """检查数据是否可用"""
    try:
        logger.info("开始数据可用性检查...")
        is_valid = data_source.validate_strategy_data()
        if not is_valid:
            logger.error("数据验证失败")
            return False
        
        test_data = data_source.load_test_data()
        if test_data is None:
            logger.error("测试数据加载失败")
            return False
        
        logger.info("数据源检查通过")
        return True
    except Exception as e:
        logger.error(f"数据源检查失败: {str(e)}")
        return False

def custom_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """自定义技术指标计算"""
    try:
        # 计算移动平均线
        df['ma5'] = df['close'].rolling(5, min_periods=1).mean()
        df['ma10'] = df['close'].rolling(10, min_periods=1).mean()
        df['ma20'] = df['close'].rolling(20, min_periods=1).mean()
        df['ma60'] = df['close'].rolling(60, min_periods=1).mean()
        df['ma200'] = df['close'].rolling(200, min_periods=1).mean()
        
        # 计算角度指标
        df['ma60_angle'] = np.degrees(np.arctan((df['ma60']/df['ma60'].shift(20)-1)*100))
        df['ma200_angle'] = np.degrees(np.arctan((df['ma200']/df['ma200'].shift(20)-1)*100))
        
        # 计算RSI
        delta = df['close'].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(RSI_PERIOD, min_periods=1).mean()
        avg_loss = loss.rolling(RSI_PERIOD, min_periods=1).mean()
        rs = np.where(avg_loss != 0, avg_gain / avg_loss, np.inf)
        df['rsi6'] = 100 - (100 / (1 + rs))
        
        # 计算威廉指标 (Williams %R)
        highest_high = df['high'].rolling(14, min_periods=1).max()
        lowest_low = df['low'].rolling(14, min_periods=1).min()
        df['williams_r'] = (highest_high - df['close']) / (highest_high - lowest_low) * -100
        
        # 计算量比
        df['vol_ma5'] = df['volume'].rolling(5, min_periods=1).mean()
        df['vol_ratio'] = df['volume'] / df['vol_ma5']
        
        # 计算偏离度
        df['deviation'] = abs(df['close'] - df['ma5']) / df['ma5']
        
        # 计算均线斜率
        df['ma5_slope'] = (df['ma5'] - df['ma5'].shift(4)) / df['ma5'] / 5
        df['ma200_slope'] = (df['ma200'] - df['ma200'].shift(20)) / df['ma200'] / 20
        
        # 计算10日涨跌幅
        df['pct_10d'] = df['close'].pct_change(10)
        
        # 增加MACD指标
        exp12 = df['close'].ewm(span=12, adjust=False).mean()
        exp26 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp12 - exp26
        df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        
        # 计算近期最低价（用于止损）
        df['low_10d'] = df['low'].rolling(10, min_periods=1).min()
        
        # 处理NaN值
        df = df.fillna(0)
        
        return df
    except Exception as e:
        logger.error(f"指标计算失败: {str(e)}")
        return df

def load_index_components(index_code: str) -> list[str]:
    """加载指数成分股"""
    try:
        constituents = data_source.get_index_constituents(index_code)
        logger.info(f"从数据源获取到指数{index_code}的成分股数量: {len(constituents) if constituents else 0}")
        if constituents:
            logger.info(f"成功获取指数{index_code}的成分股{len(constituents)}只")
            return constituents
        
        logger.warning(f"无法获取指数{index_code}成分股，将返回所有可用股票")
        all_symbols = data_source.get_all_symbols()
        logger.info(f"从数据源获取到所有股票数量: {len(all_symbols) if all_symbols else 0}")
        if not all_symbols:
            logger.error("无法获取任何股票数据")
            return []
        return all_symbols
    except Exception as e:
        logger.error(f"获取指数{index_code}成分股失败: {str(e)}")
        return []

def _get_stock_data(all_symbols: set, end_date=None) -> pd.DataFrame:
    """获取股票数据"""
    logger.info(f"正在获取选股数据(200天)...")
    
    # 如果提供了结束日期，则基于该日期往前推200天获取数据
    if end_date:
        # 计算开始日期
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        start_date = end_date_obj - timedelta(days=200)
        
        # 直接从数据源查询数据
        df = data_source.query(
            symbols=list(all_symbols),
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date,
            timeframe='1d',
            adjust=''
        )
    else:
        df = data_source.load_stock_data(list(all_symbols), days=200)
    
    if df is None or df.empty:
        logger.error("无法获取股票数据")
        return pd.DataFrame()
    
    logger.info(f"成功获取股票数据，共{len(df)}条记录")
    return df

def _filter_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """第一步：适度过滤不符合条件的股票"""
    logger.info("开始执行第一步：适度过滤明显不符合条件的股票")
    filtered_symbols = []
    
    for symbol in df['symbol'].unique():
        stock_df = df[df['symbol'] == symbol].copy()
        if stock_df.empty:
            continue
            
        # 计算技术指标
        stock_df = custom_indicators(stock_df)
        latest = stock_df.iloc[-1]
        
        # 确保关键列存在
        required_columns = ['ma5', 'ma10', 'ma20', 'ma60', 'ma200', 'ma60_angle', 'ma200_angle', 'pct_10d', 'ma5_slope', 'ma200_slope', 'low_10d']
        for col in required_columns:
            if col not in latest or pd.isna(latest[col]):
                continue
        
        # ==================== 适度剔除条件 ====================
        # 1. 剔除长期趋势明显向下的股票 (MA60角度 < -20°)
        if latest['ma60_angle'] < -20:
            continue
        
        # 2. 剔除明显空头排列的股票 (MA5 < MA10 < MA20 < MA60)
        if (latest['ma5'] < latest['ma10'] < latest['ma20'] < latest['ma60'] and
            latest['ma5']/latest['ma60'] < 0.95):  # 更严格的阈值
            continue
        
        # 3. 剔除短期涨幅过大的股票 (涨幅限制适当放宽)
        if latest['pct_10d'] > 0.7:  # 从50%提高到70%
            continue
        
        # 4. 剔除均线明显向下偏离的股票 (标准放宽)
        if latest['ma5_slope'] < -0.003:  # 从0.1%提高到0.3%
            continue
            
        # 通过过滤条件
        filtered_symbols.append(symbol)
    
    logger.info(f"过滤后剩余股票数量: {len(filtered_symbols)}/{df['symbol'].nunique()}")
    return df[df['symbol'].isin(filtered_symbols)]

def _score_stocks(df: pd.DataFrame) -> tuple:
    """第二步：对通过过滤的股票进行评分"""
    logger.info("开始执行第二步：对通过过滤的股票进行评分")
    stock_scores = []
    score_details = []
    
    for symbol in df['symbol'].unique():
        stock_df = df[df['symbol'] == symbol].copy()
        if stock_df.empty:
            continue
            
        # 确保指标已计算
        if 'ma60_angle' not in stock_df.columns:
            stock_df = custom_indicators(stock_df)
        
        latest = stock_df.iloc[-1]
        
        # 获取技术指标评分
        score = calculate_technical_score(stock_df)
        
        if score > 0:
            details = {
                'symbol': symbol,
                'score': score,
                'close': latest.get('close', 0),
                'ma5': latest.get('ma5', 0),
                'ma60_angle': latest.get('ma60_angle', 0),
                'ma200_angle': latest.get('ma200_angle', 0),
                'rsi6': latest.get('rsi6', 50),
                'deviation': latest.get('deviation', 0),
                'ma5_slope': latest.get('ma5_slope', 0),
                'ma200_slope': latest.get('ma200_slope', 0),
                'pct_10d': latest.get('pct_10d', 0),
                'low_10d': latest.get('low_10d', 0),  # 用于止损
            }
            score_details.append(details)
            stock_scores.append({
                'symbol': symbol,
                'score': score,
                'stop_loss': latest.get('low_10d', 0)  # 增加止损价位
            })
    
    return stock_scores, score_details

def _generate_score_report(score_details: list) -> None:
    """生成评分报告"""
    if not score_details:
        return
        
    details_df = pd.DataFrame(score_details)
    
    # 根据评分提供仓位配置建议
    strong_stocks = details_df[details_df['score'] > 70]  # 强势区
    neutral_stocks = details_df[(details_df['score'] >= 50) & (details_df['score'] <= 70)]  # 中性区
    weak_stocks = details_df[details_df['score'] < 50]  # 弱势区
    
    report = f"""
    === 选股分析报告 ===
    股票总数: {len(details_df)}
    平均MA60角度: {details_df['ma60_angle'].mean():.2f}°
    平均偏离度: {details_df['deviation'].mean():.2%}
    平均10日涨幅: {details_df['pct_10d'].mean():.2%}
    
    仓位配置建议:
    强势区股票({len(strong_stocks)}只): 建议配置60%资金
    中性区股票({len(neutral_stocks)}只): 建议配置30%资金
    弱势区股票({len(weak_stocks)}只): 建议保留10%现金
    
    评分分布:
    {details_df['score'].value_counts().sort_index().to_string()}
    
    前10名股票:
    {details_df.sort_values('score', ascending=False).head(10).to_string(index=False)}
    """
    
    logger.info(report)
    report_path = './data/selection_report.txt'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    logger.info(f"详细选股报告已保存至: {report_path}")

def _select_top_stocks(stock_scores: list, target_count: int) -> pd.DataFrame:
    """选择评分最高的股票"""
    scores_df = pd.DataFrame(stock_scores)
    if scores_df.empty:
        logger.warning("没有符合筛选条件的股票")
        return pd.DataFrame()
    
    # 严格按分数从高到低排序
    scores_df = scores_df.sort_values('score', ascending=False)
    
    # 检查是否达到最小选股数量
    min_stock_count = CONFIG.min_stock_count
    if len(scores_df) < min_stock_count:
        logger.warning(f"选出的股票数量({len(scores_df)})少于最小要求数量({min_stock_count})")
        # 可以选择返回空DataFrame或者返回所有可用股票
        # 这里选择返回所有可用股票，但记录警告信息
        selected = scores_df
    else:
        # 选择前target_count只股票（如果可用股票少于target_count只，则选择所有可用股票）
        selected_count = min(target_count, len(scores_df))
        selected = scores_df.head(selected_count)
    
    if len(selected) < target_count:
        logger.warning(f"选出的股票数量({len(selected)})少于目标数量({target_count})")
    
    logger.info(f"选股完成，共选出{len(selected)}只股票")
    return selected

def calculate_technical_score(df: pd.DataFrame) -> float:
    """技术指标评分函数 - 仅保留贴线、威廉和RSI指标"""
    if df.empty:
        return 0
        
    latest = df.iloc[-1]
    
    # 确保关键列存在
    required_columns = ['rsi6', 'williams_r', 'deviation', 'ma5', 'close']
    for col in required_columns:
        if col not in latest or pd.isna(latest[col]):
            return 0
    
    # ==================== 简化技术指标评分 ====================
    # 威廉指标 (30分)
    williams_r = latest['williams_r']
    if -20 <= williams_r <= 0:  # 强势区域
        williams_score = 30
    elif -40 <= williams_r < -20:  # 中等强势
        williams_score = 20
    elif -60 <= williams_r < -40:  # 中性
        williams_score = 10
    elif -80 <= williams_r < -60:  # 弱势
        williams_score = 5
    else:  # 超卖区域
        williams_score = 0
    
    # RSI指标 (30分)
    rsi = latest['rsi6']
    if 55 <= rsi <= 70:  # 适中的强势区域
        rsi_score = 30
    elif 40 <= rsi < 55:  # 中性偏强
        rsi_score = 20
    elif 70 < rsi <= 80:  # 较强但可能回调
        rsi_score = 25
    elif rsi > 80:  # 超买，可能回调
        rsi_score = 10
    else:  # 超卖或极弱
        rsi_score = 0
    
    # 贴线指标 (40分)
    # 偏离度越小越好，贴线越紧越好
    deviation = latest['deviation']
    if deviation <= 0.01:  # ≤1%
        deviation_score = 40
    elif deviation <= 0.02:  # ≤2%
        deviation_score = 30
    elif deviation <= 0.03:  # ≤3%
        deviation_score = 20
    elif deviation <= 0.05:  # ≤5%
        deviation_score = 10
    else:  # >5%
        deviation_score = 0
    
    # 计算总分 (威廉指标30分 + RSI指标30分 + 贴线指标40分)
    total_score = williams_score + rsi_score + deviation_score
    
    # 止损机制检查
    # 如果股票处于超卖区域，降低评分以规避风险
    if rsi < 30 or williams_r < -80:
        logger.info(f"股票{df.iloc[0]['symbol']}处于超卖区域，RSI: {rsi}, 威廉指标: {williams_r}，降低评分")
        total_score *= 0.5  # 降低50%评分
    
    # 确保总分不超过100分
    total_score = min(100.0, total_score)
    
    return total_score

def select_stocks(target_count=None, selection_date=None) -> tuple[pd.DataFrame, list]:
    """选股主函数"""
    # 如果selection_date是datetime对象，转换为字符串
    if selection_date is not None and hasattr(selection_date, 'strftime'):
        selection_date = selection_date.strftime('%Y-%m-%d')
        
    # 如果没有提供selection_date，则使用今天日期
    if selection_date is None:
        selection_date = datetime.today().strftime('%Y-%m-%d')
        logger.info(f"未提供选股日期，使用当前日期: {selection_date}")
        
    target_count = target_count or SELECT_COUNT
    # 确保target_count是整数类型
    if isinstance(target_count, str):
        target_count = int(target_count)
        
    logger.info(f"开始执行选股策略，目标股票数量: {target_count}")
    
    if not check_data_availability():
        logger.error("数据源不可用，无法执行选股策略")
        return pd.DataFrame(), []
    
    logger.info("正在获取强势指数...")
    # 基于selection_date计算强势指数
    top_indexes = get_top_n_strong_indexes(INDEX_LIST, STRONG_INDEX_COUNT, selection_date)
    all_symbols = set()
    
    logger.info("正在获取强势指数成分股...")
    for idx in top_indexes:
        symbols = load_index_components(idx)
        if symbols:
            logger.info(f"指数{idx}包含{len(symbols)}只成分股")
            all_symbols.update(symbols)
    
    if not all_symbols:
        logger.warning("未能从强势指数中获取任何成分股，尝试使用所有股票...")
        all_symbols = set(data_source.get_all_symbols())
        if not all_symbols:
            logger.error("无法获取任何股票数据，选股失败")
            return pd.DataFrame(), []
    
    logger.info(f"选股池构建完成，共包含{len(all_symbols)}只股票")
    
    # 获取原始股票数据，基于selection_date获取历史数据
    raw_df = _get_stock_data(all_symbols, selection_date)
    if raw_df.empty:
        return pd.DataFrame(), []
    
    # 第一步：适度过滤
    filtered_df = _filter_stocks(raw_df)
    
    # 第二步：对通过过滤的股票评分
    stock_scores, score_details = _score_stocks(filtered_df)
    _generate_score_report(score_details)
    
    # 选择评分最高的股票
    selected = _select_top_stocks(stock_scores, target_count)
    
    logger.info(f"最终选股结果:\n{selected}")
    # 同时返回最强指数列表，以便在输出中显示
    return selected.reset_index(drop=True), top_indexes

def save_selected_stocks(selected_df, selection_date=None):
    """保存选股结果到CSV文件"""
    try:
        selected_df = selected_df.copy()
        selected_df['date'] = selection_date or datetime.today().strftime('%Y-%m-%d')
        
        data_dir = './data'
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        file_path = os.path.join(data_dir, 'selected_stocks.csv')
        selected_df.to_csv(file_path, index=False)
        logger.info(f"选股结果已保存到: {file_path}")
    except Exception as e:
        logger.error(f"保存选股结果失败: {str(e)}")

def run_stock_selection(selection_date=None):
    """运行选股策略"""
    try:
        # 修改返回值处理以适应新的select_stocks返回值
        selection_result = select_stocks(selection_date=selection_date)
        if isinstance(selection_result, tuple):
            selected, top_indexes = selection_result
        else:
            selected = selection_result
            top_indexes = []
        
        # 显示最强指数
        if top_indexes:
            print(f"\n📈 最强的 {len(top_indexes)} 个指数:")
            index_names = {
                "000015": "红利指数",
                "399374": "中盘成长",
                "399324": "深证红利",
                "399376": "小盘成长",
                "399006": "创业板指",
                "399372": "大盘成长",
                "399321": "国证红利"
            }
            for i, index in enumerate(top_indexes, 1):
                index_name = index_names.get(index, index)
                print(f"  {i}. {index} ({index_name})")
        else:
            print("\n📈 未计算出强势指数")
        
        if selected.empty:
            print("⚠️ 未选中任何股票")
        else:
            print(f"\n🎯 最终选中股票（共{len(selected)}只）:")
            result_df = selected[['symbol', 'score']].sort_values(by='score', ascending=False)
            print(result_df.to_string(index=False))
            save_selected_stocks(selected, selection_date)
        return selected
    except Exception as e:
        logger.exception("策略执行失败")
        return pd.DataFrame()

if __name__ == '__main__':
    run_stock_selection()