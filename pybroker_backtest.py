#!/usr/bin/env python
# coding: utf-8
"""
基于PyBroker框架的回测系统，保留原有选股逻辑
"""

# ================= 1. 配置和导入 =================
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import traceback
from decimal import Decimal
from tabulate import tabulate

# 导入pybroker框架
import pybroker as pb
from pybroker import ExecContext, Strategy, StrategyConfig
from pybroker.common import FeeInfo
from pybroker.slippage import RandomSlippageModel

# 尝试导入matplotlib和seaborn
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
except ImportError:
    print("警告：未找到matplotlib/seaborn模块，图表功能可能无法使用")
    plt = None
    sns = None

# 导入配置
from config import CONFIG

# 导入数据源
from data_source import CustomDataSource

# 导入选股逻辑
from stock_selection import run_stock_selection

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pybroker_backtest.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()


# ================= 2. 配置参数 =================
class Config:
    """策略配置参数"""
    # 回测参数
    start_date = CONFIG.start_date
    end_date = CONFIG.end_date
    initial_capital = CONFIG.initial_capital
    hold_days = CONFIG.hold_days
    top_n = CONFIG.top_n
    
    # 风险参数
    max_position_size = CONFIG.max_position_size  # 单只股票最大仓位
    stop_loss = CONFIG.stop_loss  # 止损线
    
    # 交易成本
    trade_fee = CONFIG.trade_fee  # 手续费
    stamp_tax = CONFIG.stamp_tax  # 印花税
    slippage_min = CONFIG.slippage_min  # 滑点最小百分比 (0.05%)
    slippage_max = CONFIG.slippage_max  # 滑点最大百分比 (0.15%)


# ================= 优化的数据加载方案 =================
class DynamicSymbolLoader:
    """动态股票代码加载器 - 实现三阶段数据加载"""

    def __init__(self, data_source):
        self.data_source = data_source
        self.current_symbols = set()
        self.initial_symbols_loaded = False
        self.data_cache = {}
        self.cache_hits = 0
        self.cache_misses = 0

    def load_symbols(self, ctx: ExecContext):
        """三阶段动态加载股票代码"""
        current_date = ctx.dt.date()

        # 第一阶段：首次加载所有可能股票
        if not self.initial_symbols_loaded:
            all_symbols = self.data_source.get_all_symbols()
            self.current_symbols = set(all_symbols)
            self.initial_symbols_loaded = True
            return list(all_symbols)

        # 第二阶段：调仓日加载选股+持仓股票
        rebalance_dates = ctx.session.get('rebalance_dates', [])
        if current_date in rebalance_dates:
            selected_symbols = ctx.session.get(f'selected_symbols_{current_date}', [])
            positions = ctx.positions()
            holding_symbols = [pos.symbol for pos in positions] if positions else []
            needed_symbols = set(selected_symbols) | set(holding_symbols)
            new_symbols = needed_symbols - self.current_symbols
            self.current_symbols |= new_symbols
            return list(self.current_symbols)

        # 第三阶段：非调仓日只加载持仓股票
        positions = ctx.positions()
        holding_symbols = [pos.symbol for pos in positions] if positions else []
        return holding_symbols

    def load_data(self, symbols, start_date, end_date):
        """加载指定股票数据"""
        cache_key = (tuple(sorted(symbols)), start_date, end_date)

        # 检查缓存
        if cache_key in self.data_cache:
            self.cache_hits += 1
            return self.data_cache[cache_key]

        self.cache_misses += 1

        # 从数据源加载数据
        data = self.data_source.query(symbols, start_date, end_date)
        self.data_cache[cache_key] = data
        return data

    def clear_cache(self):
        """清理数据缓存"""
        self.data_cache.clear()
        self.cache_hits = 0
        self.cache_misses = 0

def select_stocks_for_date(ctx, current_date):
    """为指定日期选择股票"""
    try:
        selected_df = run_stock_selection(current_date)
        if selected_df is None or selected_df.empty:
            return [], {}

        selected_symbols = selected_df['symbol'].tolist()
        stop_loss_prices = dict(zip(selected_df['symbol'], selected_df.get('stop_loss', [])))
        return selected_symbols, stop_loss_prices

    except Exception as e:
        logger.error(f"选股过程出错: {str(e)}")
        return [], {}


# ================= 5. 策略逻辑 =================
# 提取日志记录函数
def log_debug_info(ctx, symbol, price):
    """记录调试信息"""
    logger.debug(f"执行策略 - 日期: {ctx.dt.date()}, 股票: {symbol}, 价格: {price:.2f}")

def log_rebalance_info(ctx, current_date, selected_symbols, stop_loss_prices):
    """记录调仓日信息"""
    logger.debug(f"调仓日 {current_date} - 当前股票: {ctx.symbol}")
    logger.debug(f"选股列表内容: {selected_symbols}")
    logger.debug(f"止损价格内容: {stop_loss_prices}")

def execute(ctx: ExecContext, symbol_loader):
    """策略执行逻辑
    
    Args:
        ctx: PyBroker执行上下文
        symbol_loader: 动态股票代码加载器
    """
    current_date = ctx.dt.date()
    symbol = ctx.symbol

    # === 每日记录 ===
    # 安全访问价格数据
    price = 0
    if hasattr(ctx, 'bars') and ctx.bars is not None:
        try:
            # 直接访问bars对象的close属性
            close_prices = ctx.bars.close
            # 确保是可索引对象且有数据
            if hasattr(close_prices, '__len__') and len(close_prices) > 0:
                price = close_prices[-1]
        except Exception as e:
            logger.debug(f"获取收盘价失败: {str(e)}")
    log_debug_info(ctx, symbol, price)

    # 获取调仓日期
    rebalance_dates = ctx.session.get('rebalance_dates', [])

    # === 调仓日逻辑 ===
    if current_date in rebalance_dates:
        # 从session中获取当前选股列表和止损价格
        selected_symbols_key = f'selected_symbols_{current_date.strftime("%Y-%m-%d")}'
        stop_loss_prices_key = f'stop_loss_prices_{current_date.strftime("%Y-%m-%d")}'

        # 检查session中是否包含选股结果
        logger.debug(f"Session keys: {list(ctx.session.keys())}")
        logger.debug(f"查找选股键: {selected_symbols_key}")

        if selected_symbols_key in ctx.session:
            selected_symbols = ctx.session.get(selected_symbols_key, [])
            stop_loss_prices = ctx.session.get(stop_loss_prices_key, {})
            log_rebalance_info(ctx, current_date, selected_symbols, stop_loss_prices)

            # 检查是否有选股结果
            if isinstance(selected_symbols, list) and len(selected_symbols) > 0:
                logger.debug(f"调仓日 {current_date} 有选股结果，共{len(selected_symbols)}只股票")

                # 不在选股列表中则卖出
                if ctx.long_pos() is not None and ctx.long_pos().shares > 0 and symbol not in selected_symbols:
                    ctx.sell_all_shares()
                    logger.info(f"卖出 {symbol} (调出组合)")
                    return

                # 在选股列表中但未持有
                if symbol in selected_symbols and (ctx.long_pos() is None or (ctx.long_pos() is not None and ctx.long_pos().shares == 0)):
                    # 仓位控制
                    max_position_value = ctx.total_equity * Decimal(str(Config.max_position_size))
                    target_value = min(max_position_value, ctx.cash / Decimal(str(max(len(selected_symbols), 1))))

                    # 安全访问价格数据
                    price = 0
                    if hasattr(ctx, 'bars') and ctx.bars is not None:
                        try:
                            # 直接访问bars对象的close属性
                            close_prices = ctx.bars.close
                            # 确保是可索引对象且有数据
                            if hasattr(close_prices, '__len__') and len(close_prices) > 0:
                                price = close_prices[-1]
                        except Exception as e:
                            logger.debug(f"获取收盘价失败: {str(e)}")

                    # Log intermediate values for debugging
                    logger.debug(f"Position sizing for {symbol}: Equity={ctx.total_equity}, Cash={ctx.cash}, "
                                 f"MaxPosValue={max_position_value}, TargetValue={target_value}, "
                                 f"Price={price}")
                            
                    # 计算目标股数
                    if price > 0:
                        target_shares = int(target_value / Decimal(str(price)))
                    else:
                        target_shares = 0
                        
                    # Cap shares to prevent unrealistic orders
                    max_shares = 100000  # Example cap - 限制最大持股数量为10万股
                    target_shares = min(target_shares, max_shares)
                        
                    if target_shares > 0:
                        # 设置止损价，默认为0如果未提供
                        stop_price = stop_loss_prices.get(symbol, 0)
                        ctx.stop_loss = stop_price  # Always set stop_loss, even if 0
                            
                        # 执行买入
                        ctx.buy_shares = target_shares
                        logger.info(f"买入 {symbol} | 股数: {target_shares} | 止损: {stop_price:.2f}")
                        return  # 成功买入后直接返回
            else:
                logger.info(f"调仓日 {current_date} 选股结果为空，跳过买入")
                if hasattr(ctx, 'long_pos') and ctx.long_pos is not None and ctx.long_pos.shares > 0:
                    ctx.sell_all_shares()
                    logger.info(f"卖出 {symbol} (无新选股)")
                return
        else:
            logger.info(f"调仓日 {current_date} Session中无选股结果，跳过买入")
            if ctx.long_pos is not None and ctx.long_pos.shares > 0:
                ctx.sell_all_shares()
                logger.info(f"卖出 {symbol} (无新选股)")
            return

    # === 每日止损检查 ===
    # 只有当设置了止损价且大于0时才进行止损检查
    # 安全访问价格数据
    close_price = 0
    if hasattr(ctx, 'bars') and ctx.bars is not None:
        try:
            # 直接访问bars对象的close属性
            close_prices = ctx.bars.close
            # 确保是可索引对象且有数据
            if hasattr(close_prices, '__len__') and len(close_prices) > 0:
                close_price = close_prices[-1]
        except Exception as e:
            logger.debug(f"获取收盘价失败: {str(e)}")
    if ctx.stop_loss and ctx.stop_loss > 0 and ctx.long_pos() is not None and ctx.long_pos().shares > 0 and close_price < ctx.stop_loss:
        logger.info(f"{symbol} 触发止损 (市价: {close_price:.2f} < 止损: {ctx.stop_loss:.2f})")
        ctx.sell_all_shares()
        return

    # === 每日记录持仓信息 ===
    if ctx.long_pos() is not None and ctx.long_pos().shares > 0:
        # 安全访问价格数据
        close_price = 0
        if hasattr(ctx, 'bars') and ctx.bars is not None:
            try:
                # 直接访问bars对象的close属性
                close_prices = ctx.bars.close
                # 确保是可索引对象且有数据
                if hasattr(close_prices, '__len__') and len(close_prices) > 0:
                    close_price = close_prices[-1]
            except Exception as e:
                logger.debug(f"获取收盘价失败: {str(e)}")
        position_value = ctx.long_pos().shares * close_price
        logger.debug(f"持仓 {symbol} | 股数: {ctx.long_pos().shares} | 市值: {position_value:.2f}")


from typing import Dict, Set, Any
from typing_extensions import Mapping  # 支持更早版本Python的Mapping泛型类型
from pybroker import ExecContext, Strategy, StrategyConfig

def after_rebalance(ctx_dict: Mapping[str, Any]):
    """交易日结束后执行的回调"""
    if not ctx_dict:
        return
        
    try:
        # 获取任意一个上下文对象来访问参数
        any_ctx = next(iter(ctx_dict.values()))
        
        # 安全获取当前持仓
        positions = any_ctx.positions()
        holding_symbols = set()
        
        # 类型检查：确保positions是列表类型
        if isinstance(positions, list):
            for pos in positions:
                # 类型检查：确保pos有shares和symbol属性
                if hasattr(pos, 'shares') and hasattr(pos, 'symbol'):
                    if pos.shares > 0:  # 只考虑有持仓的股票
                        holding_symbols.add(pos.symbol)
        
        # 获取最新选股
        current_date = any_ctx.dt.date()
        selected_symbols = set(any_ctx.session.get(f'selected_symbols_{current_date}', []))
        
        # 计算需要保留的股票
        keep_symbols = holding_symbols | selected_symbols
        
        # 清理不再需要的股票数据
        removed_count = len(selected_symbols) - len(keep_symbols & selected_symbols)
        if removed_count > 0:
            logger.info(f"清理{removed_count}只不再需要的股票数据")
            
    except Exception as e:
        logger.warning(f"调仓后处理出错: {e}")


# ================= 6. 回测执行 =================
def run_backtest():
    """运行回测"""
    logger.info("开始PyBroker回测...")
    
    try:
        # 自定义费用函数，只对卖出订单收取印花税
        def fee_func(fee_info: FeeInfo) -> Decimal:
            """计算交易费用（包括手续费和印花税）"""
            # 基本手续费（按交易金额的百分比）
            base_fee = fee_info.shares * fee_info.fill_price * Decimal(str(Config.trade_fee))
            
            # 如果是卖出订单，加上印花税
            if fee_info.order_type.lower() == 'sell':
                stamp_tax = fee_info.shares * fee_info.fill_price * Decimal(str(Config.stamp_tax))
                return base_fee + stamp_tax
            else:
                return base_fee
        
        # 配置策略
        config = StrategyConfig(
            initial_cash=Config.initial_capital,
            fee_mode=pb.FeeMode.PER_ORDER
        )
        
        # 创建自定义数据源
        data_source = CustomDataSource()
        
        # 创建动态加载器
        symbol_loader = DynamicSymbolLoader(data_source)
        
        # 计算调仓日期
        logger.info("计算调仓日期...")
        all_dates = pd.date_range(start=Config.start_date, end=Config.end_date, freq='B')
        rebalance_dates = [d.date() for i, d in enumerate(all_dates) if i % Config.hold_days == 0]
        logger.info(f"共{len(rebalance_dates)}个调仓日，前5个调仓日: {rebalance_dates[:5]}")
        
        # 创建策略对象
        logger.info("创建策略对象...")
        strategy = Strategy(data_source, Config.start_date, Config.end_date)
        
        # 获取所有股票代码（用于初始化）
        logger.info("获取所有股票代码...")
        all_symbols = data_source.get_all_symbols()
        if not all_symbols:
            logger.error("无法获取股票代码列表")
            return None
        
        logger.info(f"总共有{len(all_symbols)}只股票，前10只: {all_symbols[:10]}")
        
        # 添加执行逻辑 - 使用动态加载函数
        logger.info("添加执行逻辑...")
        strategy.add_execution(
            lambda ctx: execute(ctx, symbol_loader), 
            symbols=all_symbols
        )
        
        # 设置滑点模型
        logger.info("设置滑点模型...")
        slippage_model = RandomSlippageModel(
            min_pct=Config.slippage_min,
            max_pct=Config.slippage_max
        )
        strategy.set_slippage_model(slippage_model)
        
        # 设置指标
        # 注：移除了未使用的指标注册函数setup_indicators()
        # PyBroker 0.10+版本可能需要显式启用指标
        try:
            strategy.enable_indicators()
        except AttributeError:
            pass  # 如果方法不存在则忽略

        # 通过before_exec函数设置参数
        def set_rebalance_dates(ctx_dict: Mapping[str, ExecContext]):
            """将调仓日期设置到每个执行上下文的session中"""
            for ctx in ctx_dict.values():
                # 确保session是一个字典
                if not isinstance(ctx.session, dict):
                    ctx.session = {}
                # 确保rebalance_dates是一个列表
                if isinstance(rebalance_dates, list):
                    ctx.session['rebalance_dates'] = rebalance_dates
                else:
                    ctx.session['rebalance_dates'] = []
        
        strategy.set_before_exec(set_rebalance_dates)
        
        # 设置调仓后执行逻辑
        strategy.set_after_exec(after_rebalance)
        
        # 获取所有股票代码（使用三阶段加载）
        logger.info("初始化三阶段数据加载器...")
        
        # 运行回测
        logger.info("开始执行回测...")
        result = strategy.backtest()
        logger.info("回测执行完成")
        
        return result
        
    except Exception as e:
        logger.error(f"回测执行出错: {str(e)}")
        logger.error(traceback.format_exc())
        return None


# ================= 7. 结果分析 =================
def analyze_results(result):
    """分析回测结果"""
    if result is None:
        logger.error("回测结果为空")
        return
    
    try:
        logger.info("\n========== 策略表现 ==========")
        
        # 初始化指标字典
        metrics = {}
        
        # 使用正确的属性名访问回测结果
        if hasattr(result, 'portfolio'):
            # portfolio是DataFrame，需要使用列名访问而不是索引
            portfolio_df = result.portfolio
            # 确保portfolio_df是DataFrame类型
            if isinstance(portfolio_df, pd.DataFrame) and not portfolio_df.empty:
                # 获取期初资产
                initial_row = portfolio_df.iloc[0]  # 使用iloc[0]获取第一行
                # 确保initial_row是pandas Series类型
                if isinstance(initial_row, pd.Series):
                    initial_cash = initial_row['cash'] if 'cash' in initial_row.index else 0
                    logger.info(f"期初资产: {initial_cash:.2f}")
                    metrics['期初资产'] = f"{initial_cash:.2f}"
                
                # 获取期末资产
                final_row = portfolio_df.iloc[-1]  # 使用iloc[-1]获取最后一行
                # 确保final_row是pandas Series类型
                if isinstance(final_row, pd.Series):
                    final_cash = final_row['cash'] if 'cash' in final_row.index else 0
                    final_equity = final_row['equity'] if 'equity' in final_row.index else 0
                    final_value = final_cash + final_equity
                    logger.info(f"期末资产: {final_value:.2f}")
                    metrics['期末资产'] = f"{final_value:.2f}"
                    
                    # 计算收益率
                    if initial_cash > 0:
                        total_return = (final_value - initial_cash) / initial_cash
                        logger.info(f"总收益率: {total_return:.2%}")
                        metrics['总收益率'] = f"{total_return:.2%}"
            else:
                logger.info("无法获取详细资产信息")
                metrics['状态'] = "无法获取详细资产信息"
        else:
            logger.info("无法获取详细资产信息")
            metrics['状态'] = "无法获取详细资产信息"
            
        # 访问其他可用的属性
        if hasattr(result, 'total_return'):
            logger.info(f"总收益率: {result.total_return:.2%}")
            metrics['总收益率'] = f"{result.total_return:.2%}"
        if hasattr(result, 'annual_return'):
            logger.info(f"年化收益率: {result.annual_return:.2%}")
            metrics['年化收益率'] = f"{result.annual_return:.2%}"
        if hasattr(result, 'sharpe'):
            logger.info(f"夏普比率: {result.sharpe:.2f}")
            metrics['夏普比率'] = f"{result.sharpe:.2f}"
        if hasattr(result, 'max_drawdown'):
            logger.info(f"最大回撤: {result.max_drawdown:.2%}")
            metrics['最大回撤'] = f"{result.max_drawdown:.2%}"
        
        # 交易分析
        if hasattr(result, 'orders') and isinstance(result.orders, pd.DataFrame) and not result.orders.empty:
            logger.info("\n========== 交易分析 ==========")
            logger.info(f"总交易次数: {len(result.orders)}")
            metrics['总交易次数'] = len(result.orders)
            
            # 止损分析
            if 'type' in result.orders.columns:
                sell_orders = result.orders[result.orders['type'] == 'sell']
                logger.info(f"卖出交易次数: {len(sell_orders)}")
                metrics['卖出交易次数'] = len(sell_orders)
                
                # 添加止损交易统计
                if 'stop_loss' in result.orders.columns:
                    stop_loss_orders = result.orders[
                        (result.orders['type'] == 'sell') & 
                        (result.orders['stop_loss'] == True)
                    ]
                    stop_loss_count = len(stop_loss_orders)
                    logger.info(f"止损交易次数: {stop_loss_count}")
                    metrics['止损交易次数'] = stop_loss_count
                    
                    # 计算止损胜率
                    if len(sell_orders) > 0:
                        win_rate = 1 - (stop_loss_count / len(sell_orders))
                        logger.info(f"止损胜率: {win_rate:.2%}")
                        metrics['止损胜率'] = f"{win_rate:.2%}"
        
        # 保存交易记录
        if hasattr(result, 'orders') and not result.orders.empty:
            result.orders.to_csv('pybroker_trading_records.csv', index=False)
            logger.info("交易记录已保存至 pybroker_trading_records.csv")
        else:
            logger.warning("无交易记录可保存")
            
        # 生成表格形式的指标报告
        logger.info("\n========== 策略指标表格 ==========")
        metrics_table = [[key, value] for key, value in metrics.items()]
        logger.info("\n" + tabulate(metrics_table, headers=["指标", "数值"], tablefmt="grid"))
        
        # 生成可视化图表
        if hasattr(result, 'portfolio') and not result.portfolio.empty:
            generate_visualizations(result)
            
    except Exception as e:
        logger.error(f"结果分析出错: {str(e)}")
        logger.error(traceback.format_exc())


def generate_visualizations(result):
    """生成可视化图表"""
    try:
        # 设置中文字体
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        
        # 创建图表目录
        if not os.path.exists('charts'):
            os.makedirs('charts')
        
        # 净值曲线图
        portfolio_df = result.portfolio.copy()
        
        # 修复日期处理问题
        # 将索引重置为列，然后检查是否存在日期列
        portfolio_df = portfolio_df.reset_index()
        
        # 查找日期列
        date_col = None
        for col in portfolio_df.columns:
            if 'date' in col.lower():
                date_col = col
                break
        
        # 如果找不到日期列，使用索引作为日期
        if date_col is None:
            portfolio_df['date'] = portfolio_df.index
            date_col = 'date'
        
        # 确保日期列是datetime类型
        portfolio_df[date_col] = pd.to_datetime(portfolio_df[date_col])
        portfolio_df = portfolio_df.set_index(date_col)
        
        # 计算累计收益
        portfolio_df['total_value'] = portfolio_df['cash'] + portfolio_df['equity']
        portfolio_df['cumulative_return'] = (portfolio_df['total_value'] / portfolio_df['total_value'].iloc[0]) - 1
        
        # 创建图表
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # 净值曲线
        ax1.plot(portfolio_df.index, portfolio_df['total_value'], linewidth=2, label='策略净值')
        ax1.set_title('策略净值曲线', fontsize=16)
        ax1.set_xlabel('日期')
        ax1.set_ylabel('净值')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 收益率曲线
        ax2.plot(portfolio_df.index, portfolio_df['cumulative_return'] * 100, linewidth=2, color='red', label='累计收益率')
        ax2.set_title('累计收益率曲线', fontsize=16)
        ax2.set_xlabel('日期')
        ax2.set_ylabel('收益率 (%)')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('charts/performance_chart.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info("图表已保存至 charts/performance_chart.png")
        
        # 如果有交易记录，绘制交易点
        if hasattr(result, 'orders') and not result.orders.empty:
            orders_df = result.orders.copy()
            
            # 确保订单日期是datetime类型
            if 'date' in orders_df.columns:
                try:
                    orders_df['date'] = pd.to_datetime(orders_df['date'])
                except Exception as e:
                    logger.warning(f"转换订单日期时出错: {str(e)}")
                    orders_df['date'] = pd.to_datetime(orders_df.index)
            
            # 创建交易点图表
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # 只有在portfolio_df有效时才绘制净值曲线
            if not portfolio_df.empty and 'total_value' in portfolio_df.columns:
                ax.plot(portfolio_df.index, portfolio_df['total_value'], linewidth=2, label='策略净值')
            
            # 标记买入点
            if not orders_df.empty and 'type' in orders_df.columns:
                buy_orders = orders_df[orders_df['type'] == 'buy']
                for idx, order in buy_orders.iterrows():
                    date = order['date']
                    # 使用最近的净值数据
                    if not portfolio_df.empty:
                        try:
                            value = portfolio_df.loc[date]['total_value']
                        except:
                            # 如果找不到精确日期，使用最接近的净值
                            mask = portfolio_df.index <= date
                            if mask.any():
                                value = portfolio_df.loc[mask].iloc[-1]['total_value']
                            else:
                                value = portfolio_df['total_value'].iloc[0]
                    else:
                        value = 0
                    
                    ax.scatter(date, value, 
                              color='green', marker='^', s=100, alpha=0.7, 
                              label='买入' if idx == buy_orders.index[0] else "")
                
                # 标记卖出点
                sell_orders = orders_df[orders_df['type'] == 'sell']
                for idx, order in sell_orders.iterrows():
                    date = order['date']
                    # 使用最近的净值数据
                    if not portfolio_df.empty:
                        try:
                            value = portfolio_df.loc[date]['total_value']
                        except:
                            # 如果找不到精确日期，使用最接近的净值
                            mask = portfolio_df.index <= date
                            if mask.any():
                                value = portfolio_df.loc[mask].iloc[-1]['total_value']
                            else:
                                value = portfolio_df['total_value'].iloc[0]
                    else:
                        value = 0
                    
                    ax.scatter(date, value, 
                              color='red', marker='v', s=100, alpha=0.7, 
                              label='卖出' if idx == sell_orders.index[0] else "")
            
            ax.set_title('策略净值与交易点', fontsize=16)
            ax.set_xlabel('日期')
            ax.set_ylabel('净值')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            plt.tight_layout()
            plt.savefig('charts/trading_points_chart.png', dpi=300, bbox_inches='tight')
            plt.close()
            logger.info("交易点图表已保存至 charts/trading_points_chart.png")
            
    except Exception as e:
        logger.error(f"生成可视化图表时出错: {str(e)}")
        logger.error(traceback.format_exc())


# ================= 8. 主函数 =================
def main(*args, **kwargs):
    """主函数"""
    try:
        logger.info("========== 策略回测开始 ==========")
        
        # 运行回测
        backtest_result = run_backtest()
        
        if backtest_result is not None:
            # 分析结果
            analyze_results(backtest_result)
            
            # 显示最终资产
            if hasattr(backtest_result, 'portfolio'):
                portfolio_df = backtest_result.portfolio
                # 添加类型检查：确保portfolio_df是DataFrame类型
                if isinstance(portfolio_df, pd.DataFrame) and not portfolio_df.empty:
                    final_row = portfolio_df.iloc[-1]  # 使用iloc[-1]获取最后一行
                    # 确保final_row是pandas Series类型
                    if isinstance(final_row, pd.Series):
                        final_cash = final_row['cash'] if 'cash' in final_row.index else 0
                        final_equity = final_row['equity'] if 'equity' in final_row.index else 0
                        final_value = final_cash + final_equity
                        logger.info(f"策略执行完毕，最终资产: {final_value:.2f}")
                    else:
                        logger.info("策略执行完毕，无法获取最终资产信息")
                else:
                    logger.info("策略执行完毕，无法获取最终资产信息")
            else:
                logger.info("策略执行完毕，无法获取最终资产信息")
        else:
            logger.error("回测执行失败，无结果可分析")
            
        logger.info("========== 回测完成 ==========")
        
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()