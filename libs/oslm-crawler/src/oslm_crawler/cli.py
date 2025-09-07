
import sys


def main() -> None:
    print("Hello from oslm-crawler!")
    tmp_executor(target_org=["BAAI", "ShanghaiAILab"])

def tmp_executor(target_org=None):
    from loguru import logger
    from tqdm import tqdm
    from pathlib import Path
    from .pipeline.readers import OrgLinksReader
    from .pipeline.crawlers import HFRepoPageCrawler, MSRepoPageCrawler
    from .pipeline.crawlers import HFDetailPageCrawler, MSDetailPageCrawler
    from .pipeline.crawlers import OpenDataLabCrawler, BAAIDatasetsCrawler
    from .pipeline.writers import JsonlineWriter
    from datetime import datetime
    
    cur_path = Path(__file__)
    data_path = cur_path.parents[2] / 'data/2025-09-07'
    data_path.mkdir(exist_ok=True)
    screenshot_path = cur_path.parents[2] / 'screenshots/2025-09-07'
    log_path = cur_path.parents[2] / 'logs' / f'tmp-{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")
    logger.add(log_path, level="DEBUG")
    
    reader = OrgLinksReader(orgs=target_org)
    reader.parse_input()
    org_links = next(reader.run())
    logger.info(f"Target sources: {org_links.data['target_sources']}")
    logger.info(f"Target orgs: {org_links.message['target_orgs']}")
    logger.info(f"Total links: {org_links.message['total_links']}")
    from pprint import pprint
    pprint(org_links.data)

    hf_repo = HFRepoPageCrawler(threads=4)
    hf_repo.parse_input(org_links)
    data_list = []
    for data in hf_repo.run():
        if data.data is None:
            logger.error(f"Error in HFRepoPageCrawler with error message {data.error['error_msg']}")
            continue
        data_list.append(data)
        
    count = 0
    for data in data_list:
        if data.data is not None:
            count += len(data.data['detail_urls'])
    pbar = tqdm(total=count, desc="Crawling detail infos...")
    hf_detail = HFDetailPageCrawler(threads=4, screenshot_path=screenshot_path/'Hugging Face')
    model_writer = JsonlineWriter(data_path / 'Hugging Face/raw_models_info.jsonl', drop_keys=['repo_org_mapper'])
    dataset_writer = JsonlineWriter(data_path / 'Hugging Face/raw_datasets_info.jsonl', drop_keys=['repo_org_mapper'])
    for inp in data_list:
        hf_detail.parse_input(inp)
        for data in hf_detail.run():
            if data.data is None:
                logger.error(f"Error in HFDetailPageCrawler with error message {data.error['error_msg']}")
                pbar.update(1)
                continue
            if 'model_name' in data.data:
                model_writer.parse_input(data)
                res = next(model_writer.run())
            elif 'dataset_name' in data.data:
                dataset_writer.parse_input(data)
                res = next(dataset_writer.run())
            if res.error is not None:
                logger.error('Error in JsonlineWriter with error message')
                print(res.error['error_msg'])
            pbar.update(1)
            
    model_writer.close()
    dataset_writer.close()
    logger.info('Hugging Face done!')
    pbar.close()
    
    ms_repo = MSRepoPageCrawler(threads=4)
    ms_repo.parse_input(org_links)
    data_list = []
    for data in ms_repo.run():
        if data.data is None:
            logger.error("Error in MSRepoPageCrawler.")
            print(data.error['error_msg'])
            continue
        data_list.appen(data)
    
    count = 0
    for data in data_list:
        if data.data is not None:
            count += len(data.data['detail_urls'])
    pbar = tqdm(total=count, desc="Crawling detail infos modelscope...")
    ms_detail = MSDetailPageCrawler(threads=4, screenshot_path=screenshot_path/'ModelScope')
    model_writer = JsonlineWriter(data_path / 'ModelScope/raw_models_info.jsonl', drop_keys=['repo_org_mapper'])
    dataset_writer = JsonlineWriter(data_path / 'MOdelScope/raw_datasets_info.jsonl', drop_keys=['repo_org_mapper'])
    for inp in data_list:
        ms_detail.parse_input(inp)
        for data in ms_detail.run():
            if data.data is None:
                logger.error("Error in MSDetailPageCrawler")
                print(data.error['error_msg'])
                pbar.update(1)
                continue
            if 'model_name' in data.data:
                model_writer.parse_input(data)
                res = next(model_writer.run())
            elif 'dataset_name' in data.data:
                dataset_writer.parse_input(data)
                res = next(dataset_writer.run())
            if res.error is not None:
                logger.error('Error in JsonlineWriter (ModelScope)')
                print(res.error['error_msg'])
            pbar.update(1)
            
    model_writer.close()
    dataset_writer.close()
    logger.info("ModelScope done.")
    pbar.close()
    
    opendatalab = OpenDataLabCrawler()
    dataset_writer = JsonlineWriter(data_path/'OpenDataLab/raw_datasets_info.jsonl')
    opendatalab.parse_input(org_links)
    data_list = []
    for data in opendatalab.run():
        if data.error:
            logger.error("Error crawling open data lab dataset")
            pprint(data.error)
            continue
        dataset_writer.parse_input(data)
        res = next(dataset_writer.run())
        if res.error:
            logger.error("Error writing open data lab dataset")
            pprint(res.error)
        
    dataset_writer.close()
    logger.info("OpenDataLab done.")

    baai = BAAIDatasetsCrawler()
    dataset_writer = JsonlineWriter(data_path/'BAAIData/raw_datasets_info.jsonl') 
    baai.parse_input(org_links)
    data_list = []
    for data in baai.run():
        if data.error:
            logger.error("Error crawling baai data.")
            pprint(data.error)
            continue
        dataset_writer.parse_input(data)
        res = next(dataset_writer.run())
        if res.error:
            logger.error("Error writing baai dataset")
            pprint(res.error)
            
    dataset_writer.close()
    logger.info("BAAIData done.")
        
        
