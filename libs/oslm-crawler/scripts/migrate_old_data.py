import re
import csv
import json
import jsonlines
import shutil
from pathlib import Path
from oslm_crawler.crawler.utils import str2int


src_path = Path('/Users/liaofeng/Documents/Codespace/model_collector/results')
target_path = Path('/Users/liaofeng/Documents/Codespace/InsightSwarm/libs/oslm-crawler/data')
target_ss_path = Path('/Users/liaofeng/Documents/Codespace/InsightSwarm/libs/oslm-crawler/screenshots')
config_path = Path('/Users/liaofeng/Documents/Codespace/InsightSwarm/libs/oslm-crawler/config/org-links.json')
dataset_info_path = Path('/Users/liaofeng/Documents/Codespace/InsightSwarm/libs/oslm-crawler/config/dataset-info.json')
with config_path.open('r') as f:
    target_orgs = set(json.load(f).keys())


def trans_hf_model(src_item: dict, path: str | None, date_crawl: str) -> dict:
    tgt_item = {}
    tgt_item['repo'] = src_item['organization']
    tgt_item['model_name'] = src_item['model_name']
    tgt_item['downloads_last_month'] = str2int(src_item['downloads_last_month'])
    tgt_item['likes'] = str2int(src_item['likes'])
    tgt_item['community'] = str2int(src_item['community'])
    tgt_item['descendants'] = sum(str2int(x) for x in src_item['used_num'])
    tgt_item['date_crawl'] = date_crawl
    tgt_item['link'] = src_item['link']
    tgt_item['img_path'] = path
    tgt_item['error_msg'] = None
    tgt_item['metadata'] = {
        'downloads_last_month': src_item['downloads_last_month'],
        'likes': src_item['likes'],
        'tree': src_item['used_num'],
        'community': src_item['community']
    }
    return tgt_item


def migrate_hf_model():
    for base_path in sorted(src_path.glob('output-2025-*')):
        raw_data_path = base_path / 'raw_data/model-details-huggingface.json'
        date_crawl = re.match(r'output-([\d]+-[\d]+-[\d]+)', base_path.name).group(1)
        tgt_path = target_path / date_crawl / 'HuggingFace/raw-models-info.jsonl'
        tgt_path.parent.mkdir(parents=True, exist_ok=True)
        src_data = []
        tgt_data = []
        with raw_data_path.open('r') as f:
            src_data = json.load(f)
        for item in src_data:
            new_ss_file_name = f'{item["organization"]}_{item["model_name"]}_{date_crawl}.png'
            target_ss_file = target_ss_path / date_crawl / 'HuggingFace' / new_ss_file_name
            if target_ss_file.exists():
                target_ss_file = str(target_ss_file)
            else:
                target_ss_file = None
            tgt_data.append(trans_hf_model(item, target_ss_file, date_crawl))
        with jsonlines.open(tgt_path, 'w') as f:
            f.write_all(tgt_data)
    
            
def trans_ms_model(src_item: dict, path: str | None, date_crawl: str) -> dict:
    tgt_item = {}
    tgt_item['repo'] = src_item['organization']
    tgt_item['model_name'] = src_item['model_name']
    tgt_item['total_downloads'] = str2int(src_item['downloads'])
    tgt_item['likes'] = str2int(src_item['likes'])
    tgt_item['community'] = str2int(src_item['community'])
    tgt_item['date_crawl'] = date_crawl
    tgt_item['link'] = src_item['link']
    tgt_item['img_path'] = path
    tgt_item['error_msg'] = None
    tgt_item['metadata'] = {
        'downloads': src_item['downloads'],
        'likes': src_item['likes'],
        'community': src_item['community']
    }
    return tgt_item

    
def migrate_ms_model():
    for base_path in sorted(src_path.glob('output-2025-*')):
        raw_data_path = base_path / 'raw_data/model-details-modelscope.json'
        date_crawl = re.match(r'output-([\d]+-[\d]+-[\d]+)', base_path.name).group(1)
        tgt_path = target_path / date_crawl / 'ModelScope/raw-models-info.jsonl'
        tgt_path.parent.mkdir(parents=True, exist_ok=True)
        src_data = []
        tgt_data = []
        with raw_data_path.open('r') as f:
            src_data = json.load(f)
        for item in src_data:
            new_ss_file_name = f'{item["organization"]}_{item["model_name"]}_{date_crawl}.png'
            target_ss_file = target_ss_path / date_crawl / 'ModelScope' / new_ss_file_name
            if target_ss_file.exists():
                target_ss_file = str(target_ss_file)
            else:
                target_ss_file = None
            tgt_data.append(trans_ms_model(item, target_ss_file, date_crawl))
        with jsonlines.open(tgt_path, 'w') as f:
            f.write_all(tgt_data)


def trans_hf_model_2024(src_item: dict, path: str | None,  date_crawl: str) -> dict:
    tgt_item = {}
    tgt_item['repo'] = src_item['organization']
    tgt_item['model_name'] = src_item['model_name']
    tgt_item['downloads_last_month'] = src_item['downloads_last_month']
    tgt_item['likes'] = src_item['likes']
    tgt_item['community'] = src_item['community']
    tgt_item['descendants'] = src_item['used_num']
    tgt_item['date_crawl'] = date_crawl
    tgt_item['link'] = src_item['link']
    tgt_item['img_path'] = path
    tgt_item['error_msg'] = None
    tgt_item['metadata'] = {}
    return tgt_item


def migrate_hf_model_2024():
    for base_path in sorted(src_path.glob('output-2024-*')):
        raw_data_path = base_path / 'processed_data/model-details-huggingface.json'
        date_crawl = re.match(r'output-([\d]+-[\d]+-[\d]+)', base_path.name).group(1)
        tgt_path = target_path / date_crawl / 'HuggingFace/raw-models-info.jsonl'
        tgt_path.parent.mkdir(parents=True, exist_ok=True)
        src_data = []
        tgt_data = []
        with raw_data_path.open('r') as f:
            src_data = json.load(f)
        for item in src_data:
            new_ss_file_name = f'{item["organization"]}_{item["model_name"]}_{date_crawl}.png'
            target_ss_file = target_ss_path / date_crawl / 'HuggingFace' / new_ss_file_name
            if target_ss_file.exists():
                target_ss_file = str(target_ss_file)
            else:
                target_ss_file = None
            tgt_data.append(trans_hf_model_2024(item, target_ss_file, date_crawl))
        with jsonlines.open(tgt_path, 'w') as f:
            f.write_all(tgt_data)
   
            
def trans_ms_model_2024(src_item: dict, path: str | None, date_crawl: str) -> dict:
    tgt_item = {}
    tgt_item['repo'] = src_item['organization']
    tgt_item['model_name'] = src_item['model_name']
    tgt_item['total_downloads'] = src_item['downloads']
    tgt_item['likes'] = src_item['likes']
    tgt_item['community'] = src_item['community']
    tgt_item['date_crawl'] = date_crawl
    tgt_item['link'] = src_item['link']
    tgt_item['img_path'] = path
    tgt_item['error_msg'] = None
    tgt_item['metadata'] = {}
    return tgt_item


def migrate_ms_model_2024():
    for base_path in sorted(src_path.glob('output-2024-12*')):
        raw_data_path = base_path / 'processed_data/model-details-modelscope.json'
        date_crawl = re.match(r'output-([\d]+-[\d]+-[\d]+)', base_path.name).group(1)
        tgt_path = target_path / date_crawl / 'ModelScope/raw-models-info.jsonl'
        tgt_path.parent.mkdir(parents=True, exist_ok=True)
        src_data = []
        tgt_data = []
        with raw_data_path.open('r') as f:
            src_data = json.load(f)
        for item in src_data:
            new_ss_file_name = f'{item["organization"]}_{item["model_name"]}_{date_crawl}.png'
            target_ss_file = target_ss_path / date_crawl / 'ModelScope' / new_ss_file_name
            if target_ss_file.exists():
                target_ss_file = str(target_ss_file)
            else:
                target_ss_file = None
            tgt_data.append(trans_ms_model_2024(item, target_ss_file, date_crawl))
        with jsonlines.open(tgt_path, 'w') as f:
            f.write_all(tgt_data)
         
            
def check_link_unique(f_path: Path):
    links = set()
    dup_links = {}
    with f_path.open('r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for item in reader:
            link = item['链接']
            assert isinstance(link, str) and link != '', f'error: {link}'
            if link not in links:
                links.add(link)
            elif link not in dup_links:
                dup_links[link] = 2
            else:
                dup_links[link] += 1
    
    return dup_links

modality_mapper = {
    '语言': 'Language',
    '语音': 'Speech',
    '视觉': 'Vision',
    '多模态': 'Multimodal',
    '具身': 'Embodied',
    '评测': None
}
lifecircle_mapper = {
    '预训练': 'Pre-training',
    '微调': 'Fine-tuning',
    '偏好': 'Preference',
    '评测': 'Evaluation'
}


def trans_hf_dataset(src_item: dict, date_crawl: str):
    tgt_item = {}
    link = src_item['链接']
    tgt_item['repo'] = link.rstrip('/').split('/')[-2]
    tgt_item['dataset_name'] = link.rstrip('/').split('/')[-1]
    tgt_item['downloads_last_month'] = str2int(src_item['上月下载量'])
    tgt_item['likes'] = -1
    tgt_item['community'] = -1
    tgt_item['dataset_usage'] = -1
    tgt_item['date_crawl'] = date_crawl
    tgt_item['link'] = link
    tgt_item['img_path'] = None
    tgt_item['error_msg'] = None
    tgt_item['metadata'] = {}
    
    modality = src_item['模态']
    lifecircle = src_item['生命周期']
    if modality in modality_mapper and lifecircle in lifecircle_mapper:
        modality = modality_mapper[modality]
        lifecircle = lifecircle_mapper[lifecircle]
        return tgt_item, {
            f"{tgt_item['repo']}/{tgt_item['dataset_name']}": {
                'modality': modality,
                'lifecircle': lifecircle,
                'is_valid': True
            }
        }
    else:
        return {}, {
            f"{tgt_item['repo']}/{tgt_item['dataset_name']}": {
                'modality': None,
                'lifecircle': None,
                'is_valid': False
            }
        }


def trans_ms_dataset(src_item: dict, date_crawl: str):
    tgt_item = {}
    link = src_item['链接']
    tgt_item['repo'] = link.rstrip('/').split('/')[-2]
    tgt_item['dataset_name'] = link.rstrip('/').split('/')[-1]
    tgt_item['total_downloads'] = str2int(src_item['下载量'])
    tgt_item['likes'] = -1
    tgt_item['community'] = -1
    tgt_item['date_crawl'] = date_crawl
    tgt_item['link'] = link
    tgt_item['img_path'] = None
    tgt_item['error_msg'] = None
    tgt_item['metadata'] = {}
    modality = src_item['模态']
    lifecircle = src_item['生命周期']
    if modality in modality_mapper and lifecircle in lifecircle_mapper:
        modality = modality_mapper[modality]
        lifecircle = lifecircle_mapper[lifecircle]
        return tgt_item, {
            f"{tgt_item['repo']}/{tgt_item['dataset_name']}": {
                'modality': modality,
                'lifecircle': lifecircle,
                'is_valid': True
            }
        }
    else:
        return {}, {
            f"{tgt_item['repo']}/{tgt_item['dataset_name']}": {
                'modality': None,
                'lifecircle': None,
                'is_valid': False
            }
        }


def trans_open_data_lab_dataset(src_item: dict, date_crawl):
    tgt_item = {}
    link = src_item['链接']
    tgt_item['org'] = 'ShanghaiAILab'
    tgt_item['repo'] = link.rstrip('/').split('/')[-2]
    tgt_item['dataset_name'] = link.rstrip('/').split('/')[-1]
    tgt_item['total_downloads'] = str2int(src_item['下载量'])
    tgt_item['likes'] = -1
    tgt_item['date_crawl'] = date_crawl
    tgt_item['link'] = link
    tgt_item['metadata'] = {}

    modality = src_item['模态']
    lifecircle = src_item['生命周期']
    if modality in modality_mapper and lifecircle in lifecircle_mapper:
        modality = modality_mapper[modality]
        lifecircle = lifecircle_mapper[lifecircle]
        return tgt_item, {
            f"{tgt_item['repo']}/{tgt_item['dataset_name']}": {
                'modality': modality,
                'lifecircle': lifecircle,
                'is_valid': True
            }
        }
    else:
        return {}, {
            f"{tgt_item['repo']}/{tgt_item['dataset_name']}": {
                'modality': None,
                'lifecircle': None,
                'is_valid': False
            }
        }


def trans_baai_data_dataset(src_item: dict, date_crawl):
    tgt_item = {}
    link = src_item['链接']
    tgt_item['org'] = 'BAAI'
    tgt_item['repo'] = 'BAAI'
    tgt_item['dataset_name'] = link.rstrip('/').split('/')[-1]
    tgt_item['total_downloads'] = str2int(src_item['下载量'])
    tgt_item['likes'] = -1
    tgt_item['date_crawl'] = date_crawl
    tgt_item['link'] = link

    modality = src_item['模态']
    lifecircle = src_item['生命周期']
    if modality in modality_mapper and lifecircle in lifecircle_mapper:
        modality = modality_mapper[modality]
        lifecircle = lifecircle_mapper[lifecircle]
        return tgt_item, {
            f"{tgt_item['repo']}/{tgt_item['dataset_name']}": {
                'modality': modality,
                'lifecircle': lifecircle,
                'is_valid': True
            }
        }
    else:
        return {}, {
            f"{tgt_item['repo']}/{tgt_item['dataset_name']}": {
                'modality': None,
                'lifecircle': None,
                'is_valid': False
            }
        }


def trans_tmp_dataset(src_item: dict, date_crawl: str):
    tgt_item = {}
    tgt_item['org'] = src_item['组织']
    tgt_item['repo'] = src_item['机构']
    tgt_item['dataset_name'] = src_item['数据集名称']
    tgt_item['link'] = src_item['链接']
    modality = src_item['模态']
    lifecircle = src_item['生命周期']
    if modality in modality_mapper and lifecircle in lifecircle_mapper:
        tgt_item['modality'] = modality_mapper[modality]
        tgt_item['lifecircle'] = lifecircle_mapper[lifecircle]
        return tgt_item
    else:
        return {}
    
    
def remove_duplicates(dict_list):
    unique_list = []
    seen = set()
    
    for d in dict_list:
        t = tuple(sorted(d.items()))
        if t not in seen:
            seen.add(t)
            unique_list.append(d)
    
    return unique_list
    
    
def migrate_dataset():
    src_path = Path('/Users/liaofeng/Desktop/oslm-analysis/data')
    dataset_info = {}
    for f_path in sorted(src_path.glob('dataset-details-*')):
        hf_datasets = []
        ms_datasets = []
        odl_datasets = []
        baai_datasets = []
        tmp_datasets = []
        print(f_path)
        date_crawl = re.match(r'dataset-details-([\d]+-[\d]+-[\d]+).csv', f_path.name).group(1)
        with f_path.open('r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for item in reader:
                link = item['链接']
                if link.startswith('https://huggingface'):
                    tgt_item, cfg = trans_hf_dataset(item, date_crawl)
                    if len(tgt_item) > 0:
                        hf_datasets.append(tgt_item)
                elif link.startswith('https://modelscope'):
                    tgt_item, cfg = trans_ms_dataset(item, date_crawl)
                    if len(tgt_item) > 0:
                        ms_datasets.append(tgt_item)
                elif link.startswith('https://opendatalab'):
                    tgt_item, cfg = trans_open_data_lab_dataset(item, date_crawl)
                    if len(tgt_item) > 0:
                        odl_datasets.append(tgt_item)
                elif link.startswith('https://data.baai'):
                    tgt_item, cfg = trans_baai_data_dataset(item, date_crawl)
                    if len(tgt_item) > 0:
                        baai_datasets.append(tgt_item)
                else:
                    tgt_item = trans_tmp_dataset(item, date_crawl)
                    if len(tgt_item) > 0:
                        tmp_datasets.append(tgt_item)
                    cfg = None
                if cfg is not None:
                    dataset_info.update(cfg)
        hf_path = target_path / date_crawl / 'HuggingFace/raw-datasets-info.jsonl'
        ms_path = target_path / date_crawl / 'ModelScope/raw-datasets-info.jsonl'
        odl_path = target_path / date_crawl / 'OpenDataLab/raw-datasets-info.jsonl'
        baai_path = target_path / date_crawl / 'BAAIData/raw-datasets-info.jsonl'
        tmp_path = target_path / 'tmp_fix_datasets.jsonl'
        hf_path.parent.mkdir(exist_ok=True, parents=True)
        ms_path.parent.mkdir(exist_ok=True, parents=True)
        odl_path.parent.mkdir(exist_ok=True, parents=True)
        baai_path.parent.mkdir(exist_ok=True, parents=True)
        tmp_path.parent.mkdir(exist_ok=True, parents=True)
        with jsonlines.open(hf_path, 'w') as f:
            f.write_all(hf_datasets)
        with jsonlines.open(ms_path, 'w') as f:
            f.write_all(ms_datasets)
        with jsonlines.open(odl_path, 'w') as f:
            f.write_all(odl_datasets)
        with jsonlines.open(baai_path, 'w') as f:
            f.write_all(baai_datasets)
        with jsonlines.open(tmp_path, 'w') as f:
            tmp_datasets = remove_duplicates(tmp_datasets)
            f.write_all(tmp_datasets)
        with dataset_info_path.open('w', encoding='utf-8') as f:
            json.dump(dataset_info, f, indent=4, ensure_ascii=False)


def main():
    src_path = Path('/Users/liaofeng/Desktop/oslm-analysis/data')
    for f_path in sorted(src_path.glob('dataset-details-*')):
        res = check_link_unique(f_path)
        print(f_path.name)
        print(res)
        print()


if __name__ == '__main__':
    migrate_dataset()
