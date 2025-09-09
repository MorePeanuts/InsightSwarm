import csv
import json
from pathlib import Path

src_path = Path('/Users/liaofeng/Documents/Codespace/model_collector/results')
model_info = {}
zh_en_mapper = {
    '语言': 'Language',
    '语音': 'Speech',
    '视觉': 'Vision',
    '多模态': 'Multimodal',
    '向量': 'Vector',
    '蛋白质': 'Protein',
    '3D': '3D',
    '具身': 'Embodied'
}

for base_path in sorted(src_path.glob('output-2025-*')):
    top_all_path = base_path / 'processed_data/top_all.csv'
    with open(top_all_path, 'r', encoding='utf-8') as f:
        csv_reader = csv.DictReader(f)
        for item in csv_reader:
            link = item['link_modelscope'] or item['link_huggingface']
            if not isinstance(link, str) or link == "":
                continue
            repo = link.rstrip('/').split('/')[-2]
            model_name = item['model_name']
            modality = item['modality']
            if modality in zh_en_mapper:
                modality = zh_en_mapper[modality]
            model_info[f'{repo}/{model_name}'] = {
                'modality': modality,
                'is_large_model': True,
            }
            
config_path = Path(__file__).parents[1] / 'config/model-info.json'
with config_path.open('w', encoding='utf-8') as f:
    json.dump(model_info, f, indent=4, ensure_ascii=False)
            