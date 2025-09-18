import json
import sqlite3
import traceback
from pathlib import Path
from mcp.server.fastmcp import FastMCP


DB_FILE = Path(__file__).parents[3] / "data/oslm.db"
mcp = FastMCP("oslm-database")


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

## 表格详情

### 1. `models` 表
存储了每个月爬取的开源模型的相关数据, 具体列名和含义如下: 

| 列名                 | 含义                                         |
|----------------------|----------------------------------------------|
| `org`                | 模型所属的机构/公司/组织                       |
| `repo`               | 数据源平台账号                               |
| `model_name`         | 模型的名称 (例如: 'flan-t5-xxl')             |
| `modality`           | 模型的模态 (例如: 'Language', 'Multimodal') |
| `downloads_last_month`| 上一个月的下载量                              |
| `likes`              | 点赞数量                                     |
| `community`          | 社区活跃度/讨论数量                          |
| `descendants`        | 派生模型的数量                               |
| `date_crawl`         | 数据爬取的月份 (格式: 'YYYY-MM-DD')             |
| `date_enter_db`      | 数据入库的日期                               |

- 所有机构 (org) 包括: {models_orgs}
- 所有模态 (modality) 包括: {models_modality}
- 爬取数据的月份 (date_crawl) 包括: {models_crawl_date}
- 首次爬取数据的月份为: {models_first_date}, 最新爬取数据的月份为: {models_recent_date}

### 2. `datasets` 表
存储了每个月爬取的开源数据集的相关数据, 具体列名和含义如下:

| 列名                 | 含义                                         |
|----------------------|----------------------------------------------|
| `org`                | 数据集所属的机构/公司/组织 (例如: 'ai2') |
| `repo`               | 数据源平台账号                              |
| `dataset_name`       | 数据集的名称 (例如: 'squad_v2')              |
| `modality`           | 数据集的模态 (例如: 'Language', 'Multimodal') |
| `lifecycle`          | 数据集用于大模型的生命周期 (例如: 'Pre-training') |
| `downloads_last_month`| 上一个月的下载量                              |
| `likes`              | 点赞数量                                     |
| `community`          | 社区活跃度/讨论数量                           |
| `dataset_usage`      | 数据集被使用的次数                            |
| `date_crawl`         | 数据爬取的月份 (格式: 'YYYY-MM-DD')             |
| `date_enter_db`      | 数据入库的日期                               |

- 所有机构 (org) 包括: {datasets_orgs}
- 所有模态 (modality) 包括: {datasets_modality}
- 所有生命周期 (lifecycle) 包括: {datasets_lifecycle}
- 爬取数据的月份 (date_crawl) 包括: {datasets_crawl_date}
- 首次爬取数据的月份为: {datasets_first_date}, 最新爬取数据的月份为: {datasets_recent_date}

### 3. `status` 表
记录了 `models` 和 `datasets` 表的首次爬取数据的月份和最新爬取数据的月份。
"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT DISTINCT org FROM models")
            models_orgs = [row[0] for row in cursor.fetchall()]
            models_orgs = ", ".join(f'`{item}`' for item in models_orgs) if models_orgs else "null"
            
            cursor.execute("SELECT DISTINCT modality FROM models")
            models_modality = [row[0] for row in cursor.fetchall()]
            models_modality = ", ".join(f'`{item}`' for item in models_modality) if models_modality else "null"
            
            cursor.execute("SELECT DISTINCT date_crawl FROM models")
            models_crawl_date = [row[0] for row in cursor.fetchall()]
            models_crawl_date = ", ".join(f'`{item}`' for item in models_crawl_date) if models_crawl_date else "null"
            
            cursor.execute("SELECT first_date_crawl, last_date_crawl FROM status WHERE table_name = 'models'")
            first_date, last_date = cursor.fetchone()
            models_first_date = first_date if first_date else "null"
            models_recent_date = last_date if last_date else "null"
            
            cursor.execute("SELECT DISTINCT org FROM datasets")
            datasets_orgs = [row[0] for row in cursor.fetchall()]
            datasets_orgs = ", ".join(f'`{item}`' for item in datasets_orgs) if datasets_orgs else "null"
            
            cursor.execute("SELECT DISTINCT modality FROM datasets")
            datasets_modality = [row[0] for row in cursor.fetchall()]
            datasets_modality = ", ".join(f'`{item}`' for item in datasets_modality) if datasets_modality else "null"
            
            cursor.execute("SELECT DISTINCT lifecycle FROM datasets")
            datasets_lifecycle = [row[0] for row in cursor.fetchall()]
            datasets_lifecycle = ", ".join(f'`{item}`' for item in datasets_lifecycle) if datasets_lifecycle else "null"
            
            cursor.execute("SELECT DISTINCT date_crawl FROM datasets")
            datasets_crawl_date = [row[0] for row in cursor.fetchall()]
            datasets_crawl_date = ", ".join(f'`{item}`' for item in datasets_crawl_date) if datasets_crawl_date else "null"
            
            cursor.execute("SELECT first_date_crawl, last_date_crawl FROM status WHERE table_name = 'datasets'")
            first_date, last_date = cursor.fetchone()
            datasets_first_date = first_date if first_date else "null"
            datasets_recent_date = last_date if last_date else "null"

            return schema_description.format(
                models_orgs=models_orgs,
                models_modality=models_modality,
                models_crawl_date=models_crawl_date,
                models_first_date=models_first_date,
                models_recent_date=models_recent_date,
                datasets_orgs=datasets_orgs,
                datasets_modality=datasets_modality,
                datasets_lifecycle=datasets_lifecycle,
                datasets_crawl_date=datasets_crawl_date,
                datasets_first_date=datasets_first_date,
                datasets_recent_date=datasets_recent_date,
            )
    except Exception as e:
        err = traceback.format_exc()
        return f"Error getting database schema information: {e}.\nDetails traceback: {err}"


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
