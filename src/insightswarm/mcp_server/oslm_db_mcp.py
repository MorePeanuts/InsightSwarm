import json
import sqlite3
from pathlib import Path
from mcp.server.fastmcp import FastMCP


DB_FILE = Path(__file__).parents[3] / "data/oslm.db"
mcp = FastMCP("oslm-database")

def get_database_schema1() -> str:
    """
    获取数据库的模式（schema）信息，包括每个表的列名、含义、可用关键字段值以及数据的起止时间。
    这应该是进行任何查询之前的第一步，用以了解数据的结构和内容。

    Returns:
        str: 一个Markdown格式的字符串，详细描述了数据库的模式。
    """
    schema_description = """
# OSLM 数据库模式信息

本数据库包含了从HuggingFace等平台爬取的开源模型和数据集的月度数据。

## 数据时间范围
- **模型 (models) 数据**: 从 {models_first_date} 到 {models_last_date}
- **数据集 (datasets) 数据**: 从 {datasets_first_date} 到 {datasets_last_date}

## 可用关键字段值
- **所有机构 (org)**: {all_orgs}
- **所有爬取月份 (date_crawl)**: {all_dates}

## 表格详情

### 1. `models` 表
存储了每个月爬取的模型元数据。

| 列名                 | 含义                                         |
|----------------------|----------------------------------------------|
| `org`                | 模型所属的机构/公司/组织 (例如: 'google')    |
| `repo`               | 数据源平台的账户名 (例如: 阿里巴巴的'Qwen')   |
| `model_name`         | 模型的名称 (例如: 'flan-t5-xxl')             |
| `modality`           | 模型的模态 (例如: 'text-to-text', 'audio-to-text') |
| `downloads_last_month`| 上一个月的下载量                              |
| `likes`              | 点赞数量                                     |
| `community`          | 社区得分/指标 (假设值)                       |
| `descendants`        | 派生模型的数量 (假设值)                      |
| `date_crawl`         | 数据爬取的月份 (格式: 'YYYY-MM')             |
| `date_enter_db`      | 数据入库的日期                               |

### 2. `datasets` 表
存储了每个月爬取的数据集元数据。

| 列名                 | 含义                                         |
|----------------------|----------------------------------------------|
| `org`                | 数据集所属的机构/公司/组织 (例如: 'squad') |
| `repo`               | 数据源平台的账户名 (同上)                     |
| `dataset_name`       | 数据集的名称 (例如: 'squad_v2')              |
| `modality`           | 数据集的模态 (例如: 'question-answering')    |
| `lifecycle`          | 数据集的生命周期状态 (例如: 'stable')        |
| `downloads_last_month`| 上一个月的下载量                              |
| `likes`              | 点赞数量                                     |
| `community`          | 社区得分/指标 (假设值)                       |
| `dataset_usage`      | 数据集被使用的次数 (假设值)                  |
| `date_crawl`         | 数据爬取的月份 (格式: 'YYYY-MM')             |
| `date_enter_db`      | 数据入库的日期                               |

### 3. `status` 表
记录了 `models` 和 `datasets` 表的数据爬取状态。
"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            # 1. 分别获取 models 和 datasets 的日期范围
            cursor.execute("SELECT first_date_crawl, last_date_crawl FROM status WHERE table_name = 'models'")
            models_dates = cursor.fetchone()
            models_first_date, models_last_date = models_dates if models_dates else ("未知", "未知")

            cursor.execute("SELECT first_date_crawl, last_date_crawl FROM status WHERE table_name = 'datasets'")
            datasets_dates = cursor.fetchone()
            datasets_first_date, datasets_last_date = datasets_dates if datasets_dates else ("未知", "未知")

            # 2. 获取所有唯一的 org
            cursor.execute("SELECT org FROM models UNION SELECT org FROM datasets")
            orgs = sorted([row[0] for row in cursor.fetchall()])
            all_orgs = ", ".join(f"`{org}`" for org in orgs) if orgs else "无"

            # 3. 获取所有唯一的 date_crawl
            cursor.execute("SELECT date_crawl FROM models UNION SELECT date_crawl FROM datasets ORDER BY date_crawl")
            dates = [row[0] for row in cursor.fetchall()]
            all_dates = ", ".join(f"`{d}`" for d in dates) if dates else "无"

            return schema_description.format(
                models_first_date=models_first_date,
                models_last_date=models_last_date,
                datasets_first_date=datasets_first_date,
                datasets_last_date=datasets_last_date,
                all_orgs=all_orgs,
                all_dates=all_dates
            )
    except Exception as e:
        return f"获取数据库模式信息时出错: {e}"


@mcp.tool()
def get_database_schema() -> str:
    """
    获取数据库的模式（schema）信息，包括每个表的列名、含义以及数据的起止时间。
    这应该是进行任何查询之前的第一步，用以了解数据的结构和内容。

    Returns:
        str: 一个Markdown格式的字符串，详细描述了数据库的模式。
    """
    schema_description = """
# OSLM 数据库模式信息

本数据库包含了从HuggingFace, ModelScope等平台爬取的开源模型和数据集的月度数据。

## 数据时间范围
- **首次爬取时间**: {first_date}
- **最后一次爬取时间**: {last_date}

## 表格详情

### 1. `models` 表
存储了每个月爬取的模型元数据。

| 列名                 | 含义                                         |
|----------------------|----------------------------------------------|
| `org`                | 模型所属的机构/公司/组织                       |
| `repo`               | 数据源平台账号                               |
| `model_name`         | 模型的名称 (例如: 'flan-t5-xxl')             |
| `modality`           | 模型的模态 (例如: 'text-to-text', 'audio-to-text') |
| `downloads_last_month`| 上一个月的下载量                              |
| `likes`              | 点赞数量                                     |
| `community`          | 社区得分/指标 (假设值)                       |
| `descendants`        | 派生模型的数量 (假设值)                      |
| `date_crawl`         | 数据爬取的月份 (格式: 'YYYY-MM-DD')             |
| `date_enter_db`      | 数据入库的日期                               |

### 2. `datasets` 表
存储了每个月爬取的数据集元数据。

| 列名                 | 含义                                         |
|----------------------|----------------------------------------------|
| `org`                | 数据集所属的机构/公司/组织 (例如: 'squad') |
| `repo`               | 数据源平台账号 (例如: 'hf' 表示HuggingFace)   |
| `dataset_name`       | 数据集的名称 (例如: 'squad_v2')              |
| `modality`           | 数据集的模态 (例如: 'question-answering')    |
| `lifecycle`          | 数据集的生命周期状态 (例如: 'stable')        |
| `downloads_last_month`| 上一个月的下载量                              |
| `likes`              | 点赞数量                                     |
| `community`          | 社区得分/指标 (假设值)                       |
| `dataset_usage`      | 数据集被使用的次数 (假设值)                  |
| `date_crawl`         | 数据爬取的月份 (格式: 'YYYY-MM-DD')             |
| `date_enter_db`      | 数据入库的日期                               |

### 3. `status` 表
记录了 `models` 和 `datasets` 表的数据爬取状态。
"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MIN(first_date_crawl), MAX(last_date_crawl) FROM status")
            result = cursor.fetchone()
            first_date = result[0] if result else "未知"
            last_date = result[1] if result else "未知"
            return schema_description.format(first_date=first_date, last_date=last_date)
    except Exception as e:
        return f"获取数据库模式信息时出错: {e}"


@mcp.tool()
def query_database(sql_query: str) -> str:
    """
    对 oslm.db 数据库执行一个SQL查询语句并返回结果。
    你应该首先使用 `get_database_schema` 来理解表结构，然后再构建你的SQL查询。
    请确保你的查询语句是有效的SQLite语法。

    Args:
        sql_query (str): 要执行的SQLite查询语句。

    Returns:
        str: 一个JSON格式的字符串，其中包含了查询结果。如果查询成功但没有返回数据（例如UPDATE或INSERT），则返回一个成功的消息。如果发生错误，则返回错误信息。
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            # Enables each row of data to be accessed by column name like a dictionary.
            conn.row_factory = sqlite3.Row
            
            cursor = conn.cursor()
            cursor.execute(sql_query)
            rows = cursor.fetchall()

            if rows:
                result = [dict(row) for row in rows]
                return json.dumps(result, indent=2, ensure_ascii=False)
            else:
                conn.commit()
                return json.dumps({
                    "status": "success",
                    "message": "Query executed successfully, no data returned.",
                    "rows_affected": cursor.rowcount
                })

    except sqlite3.Error as e:
        return json.dumps({"error": f"Database query error: {e}", "query": sql_query}, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": f"Unknown error while executing query: {e}", "query": sql_query}, ensure_ascii=False)


def start():
    mcp.run(transport='stdio')
