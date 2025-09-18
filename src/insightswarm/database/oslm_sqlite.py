import sqlite3
import jsonlines
from loguru import logger
from pathlib import Path
from .oslm_record import ModelRecord, DatasetRecord
from .oslm_record import HFModelRecord, HFDatasetRecord
from .oslm_record import MSModelRecord, MSDatasetRecord
from .oslm_record import OpenDataLabRecord, BAAIDataRecord

class OSLMSqliteController:
    
    create_status_table = """
create table if not exists status (
    table_name text primary key,
    first_date_crawl text,
    last_date_crawl text
)
"""
    create_models_table = """
create table if not exists models (
    org text not null,
    repo text not null,
    model_name text not null,
    modality text,
    downloads_last_month integer,
    likes integer,
    community integer,
    descendants integer,
    date_crawl text not null,
    date_enter_db text,
    primary key (org, model_name, date_crawl)
)
"""
    create_datasets_table = """
create table if not exists datasets (
    org text not null,
    repo text not null,
    dataset_name text not null,
    modality text,
    lifecycle text,
    downloads_last_month integer,
    likes integer,
    community integer,
    dataset_usage integer,
    date_crawl text not null,
    date_enter_db text,
    primary key (org, dataset_name, date_crawl)
)
"""
    create_hf_models_table = """
"""
    create_hf_datasets_table = """
"""
    create_ms_models_table = """
"""
    create_ms_datasets_table = """
"""
    create_odl_datasets_table = """
"""
    create_baai_datasets_table = """
"""

    insert_status_table = """
insert into status (table_name, first_date_crawl, last_date_crawl)
values (?, ?, ?)
"""
    insert_models_table = """
insert into models (org, repo, model_name, modality, downloads_last_month, likes, community, descendants, date_crawl, date_enter_db)
values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
    insert_datasets_table = """
insert into datasets (org, repo, dataset_name, modality, lifecycle, downloads_last_month, likes, community, dataset_usage, date_crawl, date_enter_db)
values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

    update_status_table = """
update status set last_date_crawl = ? where table_name = ?
"""
    
    def __init__(
        self, 
        data_dir: str | None = None, 
        db_path: str | None = None,
        buffer_size: int = 64,
    ):
        if data_dir:
            self.data_dir = Path(data_dir)
        else:
            self.data_dir = Path(__file__).parents[3] / 'libs/oslm-crawler/data'
        if db_path:
            self.db_path = Path(db_path)
        else:
            self.db_path = Path(__file__).parents[3] / 'data/oslm.db'
        assert self.data_dir.exists()
        assert self.db_path.parent.exists()
        
        self.buffer_size = 64        
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        try:
            self.cursor.execute(self.create_status_table)
            self.conn.commit()
        except Exception:
            logger.exception("Exception when create status table.")
        
    def init(self):
        self._init_models_table()
        self._init_datasets_table()
    
    def update(self):
        pass
        
    def _init_models_table(self):
        try:
            self.cursor.execute(self.create_models_table)
            self.cursor.execute(self.insert_status_table, ("models", None, None))
            self.conn.commit()
        except Exception:
            logger.exception("Exception when create models table.")
            
        date_enter_db_map = {}
        buffer: list[ModelRecord] = []
        for path in sorted(self.data_dir.glob("????-??-??"))[1:]:
            data_path = path / 'merged-models-info.jsonl'
            logger.info(f"Processing {str(data_path)}...")
            with jsonlines.open(data_path, 'r') as f:
                for item in f:
                    key = f"{item['org']}/{item['model_name']}"
                    date_crawl = item['date_crawl']
                    date_enter_db = date_enter_db_map.get(key)
                    if date_enter_db is None:
                        date_enter_db_map[key] = date_crawl
                        date_enter_db = date_crawl
                    buffer.append(ModelRecord(date_enter_db=date_enter_db, **item))

                    if len(buffer) >= self.buffer_size:
                        self.cursor.executemany(self.insert_models_table, buffer)
                        buffer.clear()
            if len(buffer) > 0:
                self.cursor.executemany(self.insert_models_table, buffer)
                buffer.clear()
            self.cursor.execute(self.update_status_table, (date_crawl, "models"))
            self.conn.commit()
        
        logger.info("Init models table done.")
    
    def _init_datasets_table(self):
        try:
            self.cursor.execute(self.create_datasets_table)
            self.cursor.execute(self.insert_status_table, ("datasets", None, None))
            self.conn.commit()
        except Exception:
            logger.exception("Exception when create datasets table.")
            
        date_enter_db_map = {}
        buffer: list[DatasetRecord] = []
        for path in sorted(self.data_dir.glob("????-??-??"))[1:]:
            data_path = path / 'merged-datasets-info.jsonl'
            logger.info(f"Processing {str(data_path)}...")
            with jsonlines.open(data_path, 'r') as f:
                for item in f:
                    key = f"{item['org']}/{item['dataset_name']}"
                    date_crawl = item['date_crawl']
                    date_enter_db = date_enter_db_map.get(key)
                    if date_enter_db is None:
                        date_enter_db_map[key] = date_crawl
                        date_enter_db = date_crawl
                    buffer.append(DatasetRecord(date_enter_db=date_enter_db, **item))

                    if len(buffer) >= self.buffer_size:
                        self.cursor.executemany(self.insert_datasets_table, buffer)
                        buffer.clear()
            if len(buffer) > 0:
                self.cursor.executemany(self.insert_datasets_table, buffer)
                buffer.clear()
            self.cursor.execute(self.update_status_table, (date_crawl, "datasets"))
            self.conn.commit()
        
        logger.info("Init datasets table done.")
    
    def _init_hf_models_table(self):
        pass

    def _init_hf_datasets_table(self):
        pass

    def _init_ms_models_table(self):
        pass

    def _init_ms_datasets_table(self):
        pass

    def _init_odl_datasets_table(self):
        pass

    def _init_baai_datasets_table(self):
        pass
    
    def _init_status_table(self):
        pass
