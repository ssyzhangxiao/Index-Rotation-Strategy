# 动态市现率策略系统

该项目旨在通过技术分析指标RSI（相对强弱指数）来评估特定指数的强弱，从而实现市场择时和个股选择。

## 功能特性

- 指数强弱分析：使用RSI指标评估市场趋势
- 成分股选股：在指数成分股中筛选具有投资价值的个股
- 数据获取与处理：从外部数据源获取并清洗股票价格数据
- 回测支持：通过pybroker进行交易策略回测

## 环境要求

- Python 3.7+
- pip 包管理工具

项目依赖项：
```
pybroker>=1.2.3
akshare>=1.17.35
pandas>=1.5.0
numpy>=1.20.0
sqlalchemy>=1.4.0
psycopg2-binary>=2.9.0
```

## 快速开始

### 1. 克隆项目

```bash
git clone https://github.com/ssyzhangxiao/Index-Rotation-Strategy.git
cd 动态市现率
```

### 2. 初始化开发环境

```bash
./init_dev_env.sh
```

或者手动执行：

```bash
pip install pybroker>=1.2.3 akshare>=1.17.35 pandas>=1.5.0 numpy>=1.20.0 sqlalchemy>=1.4.0 psycopg2-binary>=2.9.0
pip install --upgrade akshare
# 创建 .env 文件（见下面的环境变量配置部分）
```

### 3. 运行程序

```bash
python main.py menu
```

## 项目结构

```
.
├── config.py                 # 配置文件
├── main.py                   # 主程序入口
├── data_fetch.py             # 数据获取模块
├── stock_selection.py        # 股票选择模块
├── pybroker_backtest.py      # 回测模块
├── menu_handler.py           # 菜单处理模块
├── data/
│   ├── price_cache/          # 价格数据缓存（已忽略）
│   ├── constituents_cache/    # 成分股数据缓存（已忽略）
│   └── ...                   # 其他数据文件
└── ...
```

## Git 多终端同步工作流

为了避免在多终端同步代码后出现环境问题，请按照以下标准流程进行操作。

### 1. 在原始终端上推送代码

#### 1.1 提交本地更改
在推送代码前，确保所有更改都已提交：

```bash
# 检查工作区状态
git status

# 添加所有更改到暂存区
git add .

# 提交更改
git commit -m "描述你的更改内容"

# 推送到远程仓库
git push origin main
```

或者使用我们提供的简化脚本：
```bash
# 使用提交脚本（会引导您完成提交过程）
./git_commit.sh
```

#### 1.2 检查远程仓库状态
确认代码已成功推送到远程仓库：

```bash
# 查看提交历史
git log --oneline -5

# 查看远程仓库信息
git remote -v
```

### 2. 在新终端上拉取代码

#### 2.1 克隆项目（首次）
如果是首次在新终端上使用项目：

```bash
# 克隆项目
git clone https://github.com/ssyzhangxiao/Index-Rotation-Strategy.git
cd Index-Rotation-Strategy
```

#### 2.2 拉取最新代码（已有项目）
如果已在新终端上有项目副本：

```bash
# 拉取最新代码
git pull origin main
```

或者使用我们提供的同步脚本：
```bash
# 使用同步脚本（会处理未提交的更改并同步最新代码）
./sync_code.sh
```

### 3. 环境配置

#### 3.1 安装项目依赖
拉取代码后，首先安装项目依赖：

```bash
# 安装依赖项
pip install pybroker>=1.2.3 akshare>=1.17.35 pandas>=1.5.0 numpy>=1.20.0 sqlalchemy>=1.4.0 psycopg2-binary>=2.9.0

# 升级 akshare 库以确保接口稳定性
pip install --upgrade akshare
```

或者使用自动化脚本：

```bash
# 使用初始化脚本
./init_dev_env.sh
```

#### 3.2 配置环境变量

创建 .env 文件并配置环境变量：

```bash
# 创建 .env 文件
cat > .env << EOF
# 数据库配置示例
DB_HOST=localhost
DB_PORT=5432
DB_NAME=stock_strategy
DB_USER=stock_user
DB_PASSWORD=stock_password

# Akshare配置
AKSHARE_TOKEN=your_token_here

# 日志级别
LOG_LEVEL=INFO
EOF

# 根据你的本地环境修改 .env 文件中的配置项
```

### 4. 解决可能的冲突

#### 4.1 检查合并冲突
如果在拉取代码时出现合并冲突：

```bash
# 检查冲突标记
grep -r "<<<<<<<" .

# 或使用 VS Code 打开项目并查找冲突标记
```

#### 4.2 解决冲突
手动解决冲突后：

```bash
# 添加解决后的文件
git add .

# 完成合并
git commit -m "解决合并冲突"
```

### 5. 验证环境

#### 5.1 运行诊断工具
检查数据完整性：

```bash
python diagnose_data_issue.py
```

#### 5.2 运行程序
测试程序是否正常运行：

```bash
python main.py menu
```

### 6. 日常工作流程

#### 6.1 开始工作前
在开始工作前，先同步远程更改：

```bash
# 拉取最新代码
git pull origin main

# 安装可能新增的依赖
pip install pybroker>=1.2.3 akshare>=1.17.35 pandas>=1.5.0 numpy>=1.20.0 sqlalchemy>=1.4.0 psycopg2-binary>=2.9.0
```

或者使用我们的同步脚本：
```bash
./sync_code.sh
```

#### 6.2 提交更改
完成工作后，提交并推送更改：

```bash
# 检查更改
git status

# 添加更改到暂存区
git add .

# 提交更改
git commit -m "描述你的更改内容"

# 推送到远程仓库
git push origin main
```

或者使用我们的提交脚本：
```bash
./git_commit.sh
```

### 7. 数据库同步（推荐）

为避免大量数据文件同步问题，建议使用数据库存储价格数据：

#### 7.1 配置数据库
根据本文档后面的"数据库同步配置建议"部分配置数据库。

#### 7.2 迁移数据
将价格数据迁移到数据库中，避免通过 Git 同步大型数据文件。

### 8. 常见问题处理

#### 8.1 依赖问题
如果遇到依赖相关问题：

```bash
# 重新安装所有依赖
pip install pybroker>=1.2.3 akshare>=1.17.35 pandas>=1.5.0 numpy>=1.20.0 sqlalchemy>=1.4.0 psycopg2-binary>=2.9.0

# 升级 akshare
pip install --upgrade akshare
```

#### 8.2 权限问题
如果脚本无法执行：

```bash
# 添加执行权限
chmod +x *.sh
```

#### 8.3 行尾符问题
如果在不同系统之间切换：

```bash
# 配置 Git 行尾符处理
git config --global core.autocrlf input
```

## 简化提交流程 (macOS)

由于您的所有终端都是 macOS 系统，项目提供了专门优化的脚本：

- 代码同步脚本: [sync_code.sh](file:///Users/mac/Downloads/动态市现率/sync_code.sh) - 在开始工作前同步最新代码
- Git提交脚本: [git_commit.sh](file:///Users/mac/Downloads/动态市现率/git_commit.sh) - 简化 Git 提交流程

### sync_code.sh 功能：
1. 检查工作区状态
2. 自动暂存或提交未保存的更改
3. 获取并拉取远程最新代码
4. 恢复暂存的更改

### git_commit.sh 功能：
1. 检查工作区状态
2. 显示将要提交的更改详情
3. 提示输入提交信息（提供默认信息）
4. 添加所有更改到暂存区
5. 提交更改
6. 推送到远程仓库

## 环境配置说明

为了避免在多终端同步代码后出现环境问题，建议按照以下标准流程进行配置：

### 检查依赖安装

项目依赖可以直接通过 pip 安装：

```bash
pip install pybroker>=1.2.3 akshare>=1.17.35 pandas>=1.5.0 numpy>=1.20.0 sqlalchemy>=1.4.0 psycopg2-binary>=2.9.0
```

主要依赖包括：
- pybroker: 用于回测功能
- akshare: 用于获取股票数据
- pandas 和 numpy: 用于数据处理
- sqlalchemy: 用于数据库操作
- psycopg2-binary: PostgreSQL数据库驱动

### 数据库配置（可选）

为了更好地管理大量数据，建议配置远程数据库：

1. 安装 PostgreSQL 数据库
2. 创建数据库和用户
3. 配置连接信息到环境变量：
   ```bash
   export DB_HOST=your_database_host
   export DB_PORT=5432
   export DB_NAME=stock_data
   export DB_USER=your_username
   export DB_PASSWORD=your_password
   ```

### 环境变量配置

项目支持通过 `.env` 文件配置环境变量。

创建 .env 文件并根据你的本地环境修改配置项：

```bash
# 数据库配置示例
DB_HOST=localhost
DB_PORT=5432
DB_NAME=stock_strategy
DB_USER=stock_user
DB_PASSWORD=stock_password

# Akshare配置
AKSHARE_TOKEN=your_token_here

# 日志级别
LOG_LEVEL=INFO
```

### 配置文件说明

项目配置主要在 [config.py](file:///Users/mac/Downloads/动态市现率/config.py) 文件中，包含以下配置项：

- 回测参数（起止日期、初始资金等）
- 数据路径配置
- 股票筛选条件
- 技术指标参数

该文件已包含完整的默认配置，一般情况下无需修改。

## 多终端同步指南

为避免在多终端同步后出现环境问题，请遵循以下流程：

1. 拉取最新代码：
   ```bash
   ./sync_code.sh
   ```

2. 安装或更新依赖：
   ```bash
   pip install pybroker>=1.2.3 akshare>=1.17.35 pandas>=1.5.0 numpy>=1.20.0 sqlalchemy>=1.4.0 psycopg2-binary>=2.9.0
   pip install --upgrade akshare
   ```

3. 检查环境配置：
   ```bash
   # 创建 .env 文件（如果不存在）
   cat > .env << EOF
   # 数据库配置示例
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=stock_strategy
   DB_USER=stock_user
   DB_PASSWORD=stock_password

   # Akshare配置
   AKSHARE_TOKEN=your_token_here

   # 日志级别
   LOG_LEVEL=INFO
   EOF
   # 编辑 .env 文件以匹配你的环境
   ```

4. 运行程序：
   ```bash
   python main.py menu
   ```

### 常见问题处理

如果遇到数据相关问题，请尝试运行诊断工具：
```bash
python diagnose_data_issue.py
```

如果需要清理缓存数据，可以运行：
```bash
python main.py clean
```

## 数据库同步配置建议

为了避免因文件大小和数量限制导致的同步问题，建议将部分数据迁移到远程数据库进行管理。

### 适合通过远程数据库同步的文件

#### 1. 结构化配置和结果文件
- [constituents.csv](file:///Users/mac/Downloads/动态市现率/data/constituents.csv) - 成分股信息
- [backtest_results.csv](file:///Users/mac/Downloads/动态市现率/data/backtest_results.csv) - 回测结果
- [selected_stocks.csv](file:///Users/mac/Downloads/动态市现率/data/selected_stocks.csv) - 选股结果
- [selection_report.txt](file:///Users/mac/Downloads/动态市现率/data/selection_report.txt) - 选股报告

#### 2. 日志和监控文件
- [download_log.json](file:///Users/mac/Downloads/动态市现率/data/download_log.json) - 下载日志
- [main.log](file:///Users/mac/Downloads/动态市现率/data/main.log) - 主程序日志

#### 3. 元数据文件
- [config.py](file:///Users/mac/Downloads/动态市现率/config.py) - 配置文件（不含敏感信息部分）

### 不适合通过远程同步的文件

#### 1. 大型价格数据文件（应存储在数据库中）
- [prices.csv](file:///Users/mac/Downloads/动态市现率/data/prices.csv) - 合并价格数据（约65MB）
- [data/price_cache/](file:///Users/mac/Downloads/动态市现率/data/price_cache/) 目录下的所有文件 - 单个股票/指数价格数据
  - 每个文件约为 100KB-500KB
  - 文件数量多（当前21个，会持续增长）

#### 2. 缓存文件
- [data/constituents_cache/](file:///Users/mac/Downloads/动态市现率/data/constituents_cache/) 目录 - 成分股缓存

### 推荐的数据库结构

#### 1. 价格数据表 (stock_prices)
```sql
CREATE TABLE stock_prices (
    date DATE,
    open DECIMAL(10, 2),
    high DECIMAL(10, 2),
    low DECIMAL(10, 2),
    close DECIMAL(10, 2),
    volume BIGINT,
    symbol VARCHAR(10),
    PRIMARY KEY (symbol, date)
);
```

#### 2. 成分股表 (constituents)
```sql
CREATE TABLE constituents (
    index_symbol VARCHAR(10),
    stock_symbol VARCHAR(10),
    stock_name VARCHAR(50),
    update_date DATE,
    PRIMARY KEY (index_symbol, stock_symbol, update_date)
);
```

#### 3. 配置和结果表
```sql
CREATE TABLE backtest_results (
    id SERIAL PRIMARY KEY,
    run_date DATE,
    strategy_name VARCHAR(50),
    initial_capital DECIMAL(15, 2),
    final_value DECIMAL(15, 2),
    total_return DECIMAL(10, 4),
    annual_return DECIMAL(10, 4),
    max_drawdown DECIMAL(10, 4),
    sharpe_ratio DECIMAL(10, 4)
);

CREATE TABLE selected_stocks (
    id SERIAL PRIMARY KEY,
    selection_date DATE,
    stock_symbol VARCHAR(10),
    stock_name VARCHAR(50),
    score DECIMAL(10, 4),
    rank INTEGER
);
```

### 同步策略建议

1. **代码文件** - 通过 Git 进行版本控制和同步
2. **配置和结果文件** - 通过数据库进行同步
3. **价格数据** - 存储在数据库中，按需查询
4. **缓存文件** - 本地生成，不进行同步

### 实施步骤

1. 设置远程数据库服务（如 PostgreSQL、MySQL）
2. 创建上述数据表
3. 修改数据读写逻辑，使用数据库替代文件读写
4. 配置数据库连接信息（建议使用环境变量）
5. 安装数据库驱动依赖（sqlalchemy 和 psycopg2-binary）