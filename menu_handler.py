#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
菜单处理模块
负责显示菜单和处理用户选择
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 从其他模块导入需要的功能
from data_fetch import (
    run_data_fetcher, 
    test_data_loading, 
    merge_batch_files,
    create_sample_data,
    strategy_params,
    DATA_DIR
)

try:
    from data_source import CustomDataSource
except ImportError as e:
    print(f"警告: 无法导入数据源模块: {e}")

# 策略模块可能不可用，所以我们使用延迟导入
STRATEGY_MODULE_AVAILABLE = False
try:
    from stock_selection import run_stock_selection
    from pybroker_backtest import run_backtest
    STRATEGY_MODULE_AVAILABLE = True
except ImportError as e:
    print(f"警告: 无法导入策略模块: {e}")

# 配置日志
logger = logging.getLogger(__name__)

def show_menu():
    """显示主菜单"""
    print("\n" + "="*60)
    print("中国A股股票数据获取与分析系统")
    print("="*60)
    print("1. 测试模式 (加载现有数据)")
    print("2. 全新下载 (从头开始下载所有数据)")
    print("3. 断点续传 (继续上次未完成的下载)")
    print("4. 补充下载 (增量下载最新数据)")
    print("5. 数据加载 (多种加载方式可选)")
    print("6. 从缓存文件重新合并数据")
    print("7. 运行选股策略")
    print("8. 设置选股参数")
    print("9. 运行回测")
    print("10. 缓存管理")
    print("0. 退出程序")
    print("-" * 60)


def get_user_choice():
    """获取用户选择"""
    max_choice = 10 if STRATEGY_MODULE_AVAILABLE else 8
    try:
        choice = int(input(f"请选择操作 (0-{max_choice}): "))
        return choice
    except ValueError:
        print("输入无效，请输入数字!")
        return -1


def get_date_range():
    """获取用户指定的日期范围"""
    print("\n请选择数据加载的时间范围:")
    print("1. 最近1年")
    print("2. 最近3年")
    print("3. 最近5年")
    print("4. 自定义范围")
    print("5. 全部数据")
    
    try:
        range_choice = int(input("请选择 (1-5): "))
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        if range_choice == 1:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        elif range_choice == 2:
            start_date = (datetime.now() - timedelta(days=365*3)).strftime('%Y-%m-%d')
        elif range_choice == 3:
            start_date = (datetime.now() - timedelta(days=365*5)).strftime('%Y-%m-%d')
        elif range_choice == 4:
            start_date = input("请输入开始日期 (YYYY-MM-DD): ")
            end_date = input("请输入结束日期 (YYYY-MM-DD): ")
        elif range_choice == 5:
            start_date = "2010-01-01"
        else:
            print("无效选择，使用默认范围(最近3年)")
            start_date = (datetime.now() - timedelta(days=365*3)).strftime('%Y-%m-%d')
            
        return start_date, end_date
    except ValueError:
        print("输入无效，使用默认范围(最近3年)")
        start_date = (datetime.now() - timedelta(days=365*3)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
        return start_date, end_date


def load_data_with_options():
    """数据加载功能，提供多种加载选项"""
    try:
        # 检查数据文件是否存在
        prices_path = os.path.join(DATA_DIR, "prices.csv")
        
        if not os.path.exists(prices_path):
            print("错误: 数据文件不存在，请先执行数据下载!")
            print("提示: 您可以通过以下方式获取数据:")
            print("  2. 全新下载")
            print("  3. 断点续传")
            print("  4. 补充下载")
            logger.error("数据文件不存在，无法加载数据")
            return None, None

        print("\n请选择数据加载方式:")
        print("1. 选择时间范围加载数据")
        print("2. 加载全部数据 (2010年至今)")
        print("3. 交互式选择时间范围")
        
        try:
            load_choice = int(input("请选择加载方式 (1-3): "))
        except ValueError:
            print("输入无效，使用默认方式(选择时间范围)")
            load_choice = 1

        # 导入并使用CustomDataSource
        from data_source import CustomDataSource
        data_source = CustomDataSource()

        if load_choice == 1:
            # 获取用户指定的日期范围
            start_date, end_date = get_date_range()
            print(f"正在加载数据，时间范围: {start_date} 至 {end_date}")
            df = data_source.query([], start_date, end_date, '1d', '')
        elif load_choice == 2:
            # 使用完整日期范围加载数据
            start_date = "2010-01-01"
            end_date = datetime.now().strftime("%Y-%m-%d")
            print(f"正在加载数据，时间范围: {start_date} 至 {end_date}")
            df = data_source.query([], start_date, end_date, '1d', '')
        elif load_choice == 3:
            # 启用交互式日期选择
            data_source._interactive_date_selection = True
            print("正在交互式加载数据...")
            df = data_source.query([], '', '', '1d', '')
        else:
            print("无效选择，使用默认方式(选择时间范围)")
            start_date, end_date = get_date_range()
            print(f"正在加载数据，时间范围: {start_date} 至 {end_date}")
            df = data_source.query([], start_date, end_date, '1d', '')

        if not df.empty:
            print(f"成功加载 {len(df)} 条记录")
            print(f"数据时间范围: {df['date'].min()} 至 {df['date'].max()}")
            print(f"股票数量: {df['symbol'].nunique()}")
            
            # 显示数据预览
            print("\n数据预览:")
            print(df.head(10))
            
            # 显示数据统计信息
            print(f"\n数据统计:")
            print(f"股票数量: {df['symbol'].nunique()}")
            print(f"日期范围: {df['date'].min()} 到 {df['date'].max()}")
            print(f"缺失值检查:")
            print(df.isnull().sum())
            
            return data_source, df
        else:
            print("警告: 未加载到任何数据")
            return None, None
    except ImportError as e:
        print(f"错误: 无法导入数据源模块: {e}")
        print("提示: 请确保data_source.py文件存在且可访问")
        logger.error(f"导入数据源模块失败: {e}")
        return None, None
    except Exception as e:
        print(f"执行过程中出现错误: {e}")
        logger.error(f"加载数据时出现错误: {e}")
        return None, None


def set_strategy_params():
    """设置策略参数"""
    if not STRATEGY_MODULE_AVAILABLE:
        print("策略模块不可用，无法设置参数")
        return
        
    try:
        print("\n=== 策略参数设置 ===")
        print("当前参数设置:")
        print(f"  初始资金: {strategy_params['initial_cash'] if strategy_params['initial_cash'] else '默认 (1,000,000)'}")
        print(f"  交易费用: {strategy_params['fee_amount'] if strategy_params['fee_amount'] else '默认 (0.0005)'}")
        print(f"  开始日期: {strategy_params['start_date'] if strategy_params['start_date'] else '默认 (2020-01-01)'}")
        print(f"  结束日期: {strategy_params['end_date'] if strategy_params['end_date'] else '默认 (2023-01-01)'}")
        
        print("\n请输入新的参数值（直接回车保持默认值）:")
        initial_cash_input = input("初始资金 (默认1000000): ").strip()
        fee_amount_input = input("交易费用 (默认0.0005): ").strip()
        start_date_input = input("开始日期 (默认2020-01-01): ").strip()
        end_date_input = input("结束日期 (默认2023-01-01): ").strip()
        
        # 更新参数
        strategy_params['initial_cash'] = float(initial_cash_input) if initial_cash_input else None
        strategy_params['fee_amount'] = float(fee_amount_input) if fee_amount_input else None
        strategy_params['start_date'] = start_date_input if start_date_input else None
        strategy_params['end_date'] = end_date_input if end_date_input else None
        
        print("\n参数已更新!")
        print("新的参数设置:")
        print(f"  初始资金: {strategy_params['initial_cash'] if strategy_params['initial_cash'] else '默认 (1,000,000)'}")
        print(f"  交易费用: {strategy_params['fee_amount'] if strategy_params['fee_amount'] else '默认 (0.0005)'}")
        print(f"  开始日期: {strategy_params['start_date'] if strategy_params['start_date'] else '默认 (2020-01-01)'}")
        print(f"  结束日期: {strategy_params['end_date'] if strategy_params['end_date'] else '默认 (2023-01-01)'}")
        
    except ValueError:
        print("输入格式错误，参数未更新!")
    except Exception as e:
        print(f"设置参数时出现错误: {e}")


def handle_menu_choice(choice):
    """处理菜单选择"""
    try:
        if choice == 0:
            print("感谢使用，再见!")
            return "exit"
            
        elif choice == 1:
            from data_fetch import test_data_loading
            test_data_loading()
                
        elif choice == 2:
            from data_fetch import run_data_fetcher
            result = run_data_fetcher(resume=False, max_stocks=0, total_batches=3)
            print("\n最终执行结果:", result)
            
        elif choice == 3:
            from data_fetch import run_data_fetcher
            result = run_data_fetcher(resume=True, max_stocks=0, total_batches=3)
            print("\n最终执行结果:", result)
            
        elif choice == 4:
            from data_fetch import run_data_fetcher
            result = run_data_fetcher(resume=True, max_stocks=0, total_batches=3)
            print("\n最终执行结果:", result)
            
        elif choice == 5:
            # 合并原来的数据加载选项5, 9, 10
            data_source, df = load_data_with_options()
            if data_source and df is not None and not df.empty:
                # 数据加载完成后提供选择菜单
                handle_post_download_choice(data_source, df)
                
        elif choice == 6:
            from data_fetch import merge_cache_files_to_prices
            merge_result = merge_cache_files_to_prices()
            print("数据合并完成!" if merge_result else "数据合并失败，请查看日志了解详情。")
            
        elif choice == 7:
            if not STRATEGY_MODULE_AVAILABLE:
                print("策略模块不可用，无法运行策略")
                return "continue"
                
            # 导入策略运行函数
            try:
                from stock_selection import run_stock_selection
                print("开始运行选股策略...")
                selected_stocks = run_stock_selection()
                print("选股完成!")
                if selected_stocks is not None and not selected_stocks.empty:
                    print(selected_stocks)
                else:
                    print("未选出任何股票")
            except ImportError as e:
                print(f"无法导入选股模块: {e}")
            except Exception as e:
                print(f"运行选股策略时出现错误: {e}")
                import traceback
                traceback.print_exc()
                
        elif choice == 8:
            if not STRATEGY_MODULE_AVAILABLE:
                print("策略模块不可用，无法设置参数")
                return "continue"
                
            # 运行回测
            try:
                from pybroker_backtest import run_backtest
                print("开始运行回测...")
                run_backtest()
                print("回测完成!")
            except ImportError as e:
                print(f"无法导入回测模块: {e}")
            except Exception as e:
                print(f"运行回测时出现错误: {e}")
                import traceback
                traceback.print_exc()
        else:
            print("无效选项，请重新选择!")
            
    except Exception as e:
        print(f"执行过程中出现错误: {e}")
        import logging
        logging.error(f"处理菜单选择时出现错误: {e}")
        
    return "continue"


def show_post_download_menu():
    """显示数据下载完成后的选择菜单"""
    print("\n" + "="*50)
    print("数据加载完成，请选择后续操作:")
    print("="*50)
    print("1. 运行选股策略")
    print("2. 运行回测")
    print("3. 显示数据统计信息")
    print("4. 导出数据到文件")
    print("5. 返回主菜单")
    print("-"*50)
    
    try:
        choice = input("请选择操作 (1-5): ").strip()
        return choice
    except KeyboardInterrupt:
        print("\n操作已取消")
        return "5"


def handle_post_download_choice(data_source, df):
    """
    处理数据下载完成后的用户选择
    
    Args:
        data_source: 数据源对象
        df: 下载的数据DataFrame
    """
    while True:
        choice = show_post_download_menu()
        if choice == "1":
            # 运行选股策略
            try:
                from stock_selection import run_stock_selection
                selected_stocks = run_stock_selection()
                if not selected_stocks.empty:
                    print("选股完成!")
                    print(selected_stocks)
                else:
                    print("选股未产生结果")
            except ImportError:
                print("策略模块不可用")
            except Exception as e:
                print(f"运行选股策略时出现错误: {e}")
                
        elif choice == "2":
            # 运行回测
            try:
                from pybroker_backtest import run_backtest
                print("开始运行回测...")
                run_backtest()
                print("回测完成!")
            except ImportError:
                print("回测模块不可用")
            except Exception as e:
                print(f"运行回测时出现错误: {e}")
                
        elif choice == "3":
            # 显示数据统计
            print(f"\n数据集大小: {len(df)} 行")
            print(f"时间范围: {df['date'].min()} 到 {df['date'].max()}")
            print(f"股票数量: {df['symbol'].nunique()}")
            print("前5行数据:")
            print(df.head())
            
        elif choice == "4":
            # 保存数据到文件
            try:
                filename = f"exported_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                df.to_csv(filename, index=False)
                print(f"数据已保存到 {filename}")
            except Exception as e:
                print(f"保存数据时出现错误: {e}")
                
        elif choice == "5":
            # 返回主菜单
            break
        else:
            print("无效选择，请重新输入")


def run_menu_loop():
    """运行菜单循环"""
    while True:
        show_menu()
        choice = get_user_choice()
        
        if choice == 1:
            # 测试模式
            from data_fetch import test_data_loading
            test_data_loading()
            
        elif choice == 2:
            # 全新下载
            from data_fetch import run_data_fetcher
            run_data_fetcher(resume=False, max_stocks=0, total_batches=3)
            
        elif choice == 3:
            # 断点续传
            from data_fetch import run_data_fetcher
            run_data_fetcher(resume=True, max_stocks=0, total_batches=3)
            
        elif choice == 4:
            # 补充下载
            from data_fetch import run_data_fetcher
            run_data_fetcher(resume=False, max_stocks=0, total_batches=1)
            
        elif choice == 5:
            # 数据加载
            load_data_with_options()
            
        elif choice == 6:
            # 数据合并
            from data_fetch import merge_cache_files_to_prices
            success = merge_cache_files_to_prices()
            if success:
                print("✅ 数据合并完成!")
            else:
                print("❌ 数据合并失败，请查看日志了解详情。")
                
        elif choice == 7:
            # 运行策略 - 重构后提供新的选项
            if STRATEGY_MODULE_AVAILABLE:
                try:
                    print("请选择策略执行方式:")
                    print("1. 运行选股策略")
                    print("2. 运行回测")
                    strategy_choice = input("请输入选项 (1-2, 默认为1): ").strip()
                    
                    if strategy_choice == "2":
                        from pybroker_backtest import run_backtest
                        run_backtest()
                    else:
                        from stock_selection import run_stock_selection
                        selected_stocks = run_stock_selection()
                        if not selected_stocks.empty:
                            print("选股完成!")
                        else:
                            print("选股未产生结果")
                except Exception as e:
                    print(f"运行策略时出现错误: {e}")
            else:
                print("❌ 策略模块不可用")
                
        elif choice == 8:
            # 设置选股参数
            if STRATEGY_MODULE_AVAILABLE:
                set_strategy_params()
            else:
                print("❌ 策略模块不可用")
                
        elif choice == 9:
            # 运行回测
            if not STRATEGY_MODULE_AVAILABLE:
                print("❌ 策略模块不可用，无法运行回测")
                continue
                
            # 运行回测
            try:
                from pybroker_backtest import run_backtest
                print("开始运行回测...")
                run_backtest()
                print("回测完成!")
            except ImportError as e:
                print(f"无法导入回测模块: {e}")
            except Exception as e:
                print(f"运行回测时出现错误: {e}")
                import traceback
                traceback.print_exc()
                
        elif choice == 10:
            # 缓存管理
            try:
                from cache_manager import main as cache_manager_main
                cache_manager_main()
            except ImportError as e:
                print(f"❌ 无法导入缓存管理模块: {e}")
                # 如果缓存管理模块不可用，使用原来的清理功能
                try:
                    from clean_price_cache import clean_all_cache
                    print("开始清理所有价格缓存文件...")
                    clean_all_cache()
                    print("✅ 价格缓存文件清理完成!")
                except ImportError as e2:
                    print(f"❌ 无法导入缓存清理模块: {e2}")
                except Exception as e2:
                    print(f"❌ 清理缓存文件时出现错误: {e2}")
                    import traceback
                    traceback.print_exc()
            except Exception as e:
                print(f"❌ 缓存管理时出现错误: {e}")
                import traceback
                traceback.print_exc()
                
        elif choice == 0:
            # 退出
            print("👋 感谢使用，再见!")
            break
            
        else:
            print("❌ 无效选项，请重新选择")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='中国A股股票数据获取与分析系统')
    parser.add_argument('--mode', choices=['menu', 'backtest', 'stock_selection'], 
                       default='menu', help='运行模式: menu(菜单模式), backtest(直接运行回测), stock_selection(直接运行选股)')
    parser.add_argument('--merge', action='store_true', help='合并价格数据文件')
    
    args = parser.parse_args()
    
    # 处理命令行参数
    if args.merge:
        try:
            print("🔄 合并价格数据文件...")
            # 使用新的数据合并方法
            print("注意: 请使用菜单选项4的新版数据下载功能来生成prices.csv文件")
        except ImportError:
            print("❌ 无法导入数据合并模块")
        except Exception as e:
            print(f"❌ 合并失败: {e}")
        sys.exit(0)
    
    if args.mode == 'backtest':
        # 直接运行回测
        if STRATEGY_MODULE_AVAILABLE:
            try:
                from pybroker_backtest import run_backtest
                print("开始运行回测...")
                run_backtest()
                print("回测完成!")
            except ImportError as e:
                print(f"无法导入回测模块: {e}")
            except Exception as e:
                print(f"运行回测时出现错误: {e}")
                import traceback
                traceback.print_exc()
        else:
            print("策略模块不可用，无法运行回测")
    elif args.mode == 'stock_selection':
        # 直接运行选股
        if STRATEGY_MODULE_AVAILABLE:
            try:
                from stock_selection import run_stock_selection
                print("开始运行选股策略...")
                selected_stocks = run_stock_selection()
                print("选股完成!")
                if selected_stocks is not None and not selected_stocks.empty:
                    print(selected_stocks)
                else:
                    print("未选出任何股票")
            except ImportError as e:
                print(f"无法导入选股模块: {e}")
            except Exception as e:
                print(f"运行选股策略时出现错误: {e}")
                import traceback
                traceback.print_exc()
        else:
            print("策略模块不可用，无法运行选股")
    else:
        # 默认菜单模式
        run_menu_loop()

if __name__ == "__main__":
    main()
