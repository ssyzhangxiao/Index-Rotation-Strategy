import json
import os

def record_failure(file_path, symbol, failure_type="stock"):
    """记录失败信息"""
    with open(file_path, "a") as f:
        f.write(f"{failure_type}: {symbol}\n")

def save_download_log(file_path, download_info):
    """保存下载日志"""
    with open(file_path, "w") as f:
        json.dump(download_info, f)

def load_download_log(file_path):
    """加载下载日志"""
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def clear_failure_files():
    """清除失败记录文件"""
    failure_files = ["failed_indexes.txt", "failed_stocks.txt"]
    for file_name in failure_files:
        file_path = os.path.join("data", file_name)
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                pass  # 忽略删除失败的情况

def load_pending_stocks():
    """加载待处理股票列表"""
    pending_file = os.path.join("data", "pending_stocks.txt")
    if os.path.exists(pending_file):
        with open(pending_file, "r") as f:
            return [line.strip() for line in f.readlines() if line.strip()]
    return []

def load_failed_tasks():
    """加载失败任务"""
    failed_indexes = []
    failed_stocks = []
    
    # 加载失败的指数
    failed_index_file = os.path.join("data", "failed_indexes.txt")
    if os.path.exists(failed_index_file):
        with open(failed_index_file, "r") as f:
            for line in f:
                if line.startswith("index:"):
                    failed_indexes.append(line.strip().split(":", 1)[1])
    
    # 加载失败的股票
    failed_stocks_file = os.path.join("data", "failed_stocks.txt")
    if os.path.exists(failed_stocks_file):
        with open(failed_stocks_file, "r") as f:
            for line in f:
                if line.startswith("stock:"):
                    failed_stocks.append(line.strip().split(":", 1)[1])
    
    return failed_indexes, failed_stocks

def save_pending_stocks(stocks):
    """保存待处理股票列表"""
    pending_file = os.path.join("data", "pending_stocks.txt")
    os.makedirs("data", exist_ok=True)
    with open(pending_file, "w") as f:
        for stock in stocks:
            f.write(f"{stock}\n")

def save_batch_state(current_batch, total_batches, completed_batches):
    """保存批次状态"""
    batch_state_file = os.path.join("data", "batch_state.txt")
    os.makedirs("data", exist_ok=True)
    state = {
        "current_batch": current_batch,
        "total_batches": total_batches,
        "completed_batches": completed_batches
    }
    with open(batch_state_file, "w") as f:
        json.dump(state, f)

def load_batch_state():
    """加载批次状态"""
    batch_state_file = os.path.join("data", "batch_state.txt")
    if os.path.exists(batch_state_file):
        try:
            with open(batch_state_file, "r") as f:
                state = json.load(f)
                return (
                    state.get("current_batch", 1),
                    state.get("total_batches", 3),
                    state.get("completed_batches", 0)
                )
        except Exception:
            return 1, 3, 0
    return 1, 3, 0

def clear_batch_state():
    """清除批次状态"""
    batch_state_file = os.path.join("data", "batch_state.txt")
    pending_stocks_file = os.path.join("data", "pending_stocks.txt")
    
    if os.path.exists(batch_state_file):
        os.remove(batch_state_file)
    
    if os.path.exists(pending_stocks_file):
        os.remove(pending_stocks_file)
