# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
import re
import json
import sqlite3
from loguru import logger
from tqdm import tqdm
from typing import NamedTuple
from pathlib import Path
from scripts.agentic.gen_modality_v1 import gen_model_modality


with open('../config/tmp-is-lm.json', 'r') as f1, open('../config/tmp-model-modality.json', 'r') as f2:
    is_lm = json.load(f1)
    model_modality = json.load(f2)


class ModelRecordHF(NamedTuple):
    repo: str
    model_name: str
    modality: str | None
    org: str
    downloads_last_month: int
    likes: int
    descendants: int
    community: int
    param_size: str | None
    date_begin: str
    date_end: str
    hf_link: str
    img_path: str | None
    

class ModelRecordMS(NamedTuple):
    repo: str
    model_name: str
    modality: str | None
    org: str
    downloads_last_month: int | None
    downloads_total: int
    likes: int
    community: int
    param_size: str | None
    date_begin: str
    date_end: str
    ms_link: str
    img_path: str | None


HF_KEYS = {
    "map": {
        "model_name": "model_name",
        "organization": "org",
        "link": "hf_link",
        "downloads_last_month": "downloads_last_month",
        "likes": "likes",
        "community": "community",
        "used_num": "descendants",
        "modality": "modality",
    },
    "extra": [
        "rep", "param_size", "date_begin", "date_end", "img_path"
    ],
    "columns": [
        "repo", "model_name", "modality", "org", "downloads_last_month", 
        "likes", "descendants", "community", "param_size", "date_begin",
        "date_end", "hf_link", "img_path"
    ]
}

MS_KEYS = {
    "map": {
        "model_name": "model_name",
        "organization": "org",
        "link": "ms_link",
        "downloads": "downloads_total",
        "community": "community",
        "modality": "modality",
        "likes": "likes"
    },
    "extra": [
        "repo", "param_size", "date_end", "img_path"
    ],
    "columns": [
        "repo", "model_name", "modality", "org", "downloads_total",
        "likes", "community", "param_size", "date_end", "ms_link", "img_path"
    ]
}

BUFFER_SIZE = 64

    
def load_hf_models(base_path: Path, date_begin: str, date_end: str) -> None:
    
    json_path = base_path / "processed_data/model-details-huggingface.json"
    img_dir = base_path / "screenshots_huggingface"
    
    with open(json_path, "r") as f:
        data = json.load(f)
        
    buffer = []
    todo_buffer = []
    insert_sql = f"""
        insert into model_hf ({', '.join(HF_KEYS['columns'])})
        values ({', '.join(['?' for _ in HF_KEYS['columns']])})
    """
        
    for item in tqdm(data):
        link = item['link'].rstrip('/')
        repo = link.split('/')[-2]
        identifier = repo + '/' + item['model_name']
        
        # is_lm
        if identifier in is_lm:
            is_lm_flag = is_lm[identifier]
        elif link in is_lm:
            is_lm_flag = is_lm[link]
        else:
            raise NotImplementedError
        
        # modality
        if 'modality' in item:
            modality = item['modality']
        elif identifier in model_modality:
            modality = model_modality[identifier]
        elif link in model_modality:
            modality = model_modality[link]
        else:
            raise NotImplementedError
        
        # img_path
        img_file_name = f"{repo}-{item['model_name']}.png"
        img_path = img_dir / img_file_name
        
        # downloads_last_month
        # TODO
        
        
        record = ModelRecordHF(
            repo=repo,
            model_name=item['model_name'],
            modality=modality,
            org=item['organization'],
            downloads_last_month=item['downloads_last_month'],
            likes=item['likes'],
            descendants=item['used_num'],
            community=item['community'],
            param_size=None,
            date_begin=date_begin,
            date_end=date_end,
            hf_link=link,
            img_path=img_path,
        )

        buffer.append(tuple(record))
        
        if len(buffer) >= BUFFER_SIZE:
            conn.executemany(insert_sql, buffer)
            buffer.clear()
            
    if len(buffer) > 0:
        conn.executemany(insert_sql, buffer)
        buffer.clear()
            
            
def load_ms_models(json_path: Path, date_begin: str, date_end: str) -> None:
    pass
            


if __name__ == "__main__":
    db_path = "database/oslm_tmp.db"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    base_path = Path("/Users/liaofeng/Documents/Codespace/model_collector/results")
    hf_json_paths = [path / "processed_data/model-details-huggingface.json" for path in base_path.iterdir() if path.is_dir()]
    ms_json_paths = [path / "processed_data/model-details-modelscope.json" for path in base_path.iterdir() if path.is_dir()]
    
    date_begin = None
    for path in base_path.iterdir():
        logger.info(f"Loading huggingface data from {path}")
        date_end = re.search(r'\d{4}-\d{2}-\d{2}', path.name).group()
        load_hf_models(path, date_begin, date_end)
        
        logger.info(f"Loading modelscope data from {path}")
        load_ms_models(path, date_begin, date_end)
        
        date_begin = date_end
    
    conn.commit()
    conn.close()
