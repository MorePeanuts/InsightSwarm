import jsonlines
from pathlib import Path


src_path = Path(__file__).parents[1] / 'data/2025-09-07/Hugging Face/raw_models_info.jsonl'
target_path = src_path.parent / 'new_models_info.jsonl'

data  = []
with jsonlines.open(src_path, 'r') as r:
    for line in r:
        line.pop('repo_org_mapper')
        data.append(line)
        
with jsonlines.open(target_path, 'w') as w:
    for line in data:
        w.write(line)
        

src_path = Path(__file__).parents[1] / 'data/2025-09-07/Hugging Face/raw_datasets_info.jsonl'
target_path = src_path.parent / 'new_datasets_info.jsonl'

data  = []
with jsonlines.open(src_path, 'r') as r:
    for line in r:
        line.pop('repo_org_mapper')
        data.append(line)
        
with jsonlines.open(target_path, 'w') as w:
    for line in data:
        w.write(line)
