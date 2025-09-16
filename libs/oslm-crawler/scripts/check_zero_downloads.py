import jsonlines
import argparse
from loguru import logger
from pathlib import Path
from tqdm import tqdm
from oslm_crawler.ai.screenshot_checker import check_image_info, CheckRequest


parser = argparse.ArgumentParser()
parser.add_argument("raw_path")
args = parser.parse_args()

path = Path(args.raw_path)
logger.add(path.parent / 'check.log', level="DEBUG")
buffer = []
count = 0
total = 0
with jsonlines.open(path, 'r') as f:
    for item in f:
        if item['total_downloads'] == 0 and item['img_path'] and Path(item['img_path']).exists():
            total += 1
pbar = tqdm(total=total, desc="Error correction...")
with jsonlines.open(path, 'r') as f:
    for item in f:
        assert 'total_downloads' in item
        if item['total_downloads'] == 0 and item['img_path'] and Path(item['img_path']).exists():
            request = CheckRequest(item['img_path'], item['link'], 'ModelScope')
            response = check_image_info([request])[0]
            pbar.update(1)
            if response.downloads is not None and response.downloads > 0:
                downloads = response.downloads
                logger.info(f"Data error: {item}, downloads corrected from {item['total_downloads']} to {downloads}")
                count += 1
                item['total_downloads'] = downloads
        buffer.append(item)
print(f'successful: {count}')
pbar.close()

with jsonlines.open(path, 'w') as f:
    f.write_all(buffer)
