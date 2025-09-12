import jsonlines
import sys
from collections import defaultdict
from typing import Literal
from loguru import logger
from oslm_crawler.pipeline.base import PipelineData
from oslm_crawler.pipeline.processors import HFInfoProcessor
from oslm_crawler.pipeline.processors import MSInfoProcessor
from oslm_crawler.pipeline.processors import OpenDataLabInfoProcessor
from oslm_crawler.pipeline.processors import BAAIDataInfoProcessor
from tqdm import tqdm
from pathlib import Path
from .pipeline.readers import OrgLinksReader, JsonlineReader
from .pipeline.crawlers import HFRepoPageCrawler, MSRepoPageCrawler
from .pipeline.crawlers import HFDetailPageCrawler, MSDetailPageCrawler
from .pipeline.crawlers import OpenDataLabCrawler, BAAIDatasetsCrawler
from .pipeline.writers import ModelDatasetJsonlineWriter, JsonlineWriter
from datetime import datetime


class HFPipeline:
    
    def __init__(
        self, 
        task_name: str,
        load_dir: str | None = None,
        save_dir: str | None = None,
        log_path: str | None = None,
    ):
        self.task_name = task_name
        self.crawl_date = datetime.now().strftime(r"%Y-%m-%d_%H-%M-%S")
        if load_dir:
            self.load_dir = Path(load_dir)
        else:
            self.load_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/HuggingFace'
        if save_dir:
            self.save_dir = Path(save_dir)
        else:
            self.save_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/HuggingFace'
        if log_path is None:
            log_path = Path(__file__).parents[2] / f'logs/{task_name}-{self.crawl_date}'/'running.log'
        else:
            log_path = Path(log_path)
        self.log_path = log_path
        self.log_path.parent.mkdir(exist_ok=True, parents=True)
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
        logger.add(log_path, level="DEBUG")
        self.error_f = log_path.parent
        
    def step_all(
        self,
        save: bool,
    ):
        return self._init_org_links(
            save
        )._crawl_repo_page(
            save        
        )._crawl_detail_page(
            save
        )._post_process(
            save
        )
    
    def step(
        self, 
        stage: Literal[
            'init_org_links',
            'crawl_repo_page',
            'crawl_detail_page',
            'post_process',
        ],
        save: bool,
        **kargs,
    ):
        match stage:
            case 'init_org_links':
                return self._init_org_links(save, **kargs)
            case 'crawl_repo_page':
                return self._crawl_repo_page(save, **kargs)
            case 'crawl_detail_page':
                return self._crawl_detail_page(save, **kargs)
            case 'post_process':
                return self._post_process(save, **kargs)
                
    def done(self):
        logger.success(f"HFPipeline {self.task_name} done.")
    
    def _init_org_links(self, save, **kargs):
        logger.info("Init org links of HuggingFace")
        kargs = {k: v for k, v in kargs.items() if k in ['path', 'orgs']}
        kargs['sources'] = ['HuggingFace']
        reader = OrgLinksReader(**kargs)
        reader.parse_input()
        res = next(reader.run())
        logger.info(f"Target orgs: {res.message['target_orgs']}")
        logger.info(f"Total links: {res.message['total_links']}")
        if save:
            save_path = self.save_dir / "org-links.jsonl"
            writer = JsonlineWriter(save_path)
            writer.parse_input(res)
            res = next(writer.run())
        writer.close()
        self._init_org_links_res = res
        return self
    
    def _crawl_repo_page(self, save, **kargs):
        error_f = self.error_f / 'org-links.jsonl'
        error_f = open(error_f, 'a')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Crawl repo page of HuggingFace")
        if not hasattr(self, "_init_org_links_res"):
            logger.info("Missing the running result of the previous step (init_org_links)")
            logger.info(f"Trying load required data from {self.load_dir}")
            reader = JsonlineReader(self.load_dir/'org-links.jsonl')
            error_list = next(reader.run()).data.get('content')
            self._init_org_links_res = PipelineData({
                "HuggingFace": [err['repo_link'] for err in error_list],
                "target_sources": ["HuggingFace"],
            }, None, None)
        inp = self._init_org_links_res
        kargs = {k: v for k, v in kargs.items() if k in ['category', 'threads', 'max_retries']}
        crawler = HFRepoPageCrawler(**kargs)
        crawler.parse_input(inp)
        count = len(crawler.input['link-category'])
        pbar = tqdm(total=count, desc="Crawling repo infos from HuggingFace...")
        res = []
        if save:
            save_path = self.save_dir / "repo-page.jsonl"
            writer = JsonlineWriter(save_path, drop_keys=['repo_org_mapper'])
        for data in crawler.run():
            if data.error is not None:
                self.error_writer.write(data.error)
                error_f.flush()
                pbar.update(1)
                continue
            pbar.write(f"{data.message['repo']} huggingface has {data.message['total_links']} {data.message['category']}.")
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)
            pbar.update(1)
        
        writer.close()
        pbar.close()
        self.error_writer.close()
        error_f.close()
        self._crawl_repo_page_res = res
        return self

    def _crawl_detail_page(self, save, **kargs):
        error_f = self.error_f / 'repo-page.jsonl'
        error_f = open(error_f, 'a')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Crawl detail page of HuggingFace")
        if not hasattr(self, "_crawl_repo_page_res"):
            logger.info("Missing the running result of the previous step (crawl_repo_page)")
            logger.info(f"Trying load required data from {self.load_dir}")
            reader = JsonlineReader(self.load_dir/'repo-page.jsonl')
            error_list = next(reader.run()).data.get('content')
            model_urls = []
            dataset_urls = []
            for err in error_list:
                if err['category'] == 'models':
                    model_urls.append(err['detail_link'])
                elif err['category'] == 'datasets':
                    dataset_urls.append(err['detail_link'])
            self._crawl_repo_page_res = [PipelineData({
                'category': 'models',
                'detail_urls': model_urls
            }, None, None), PipelineData({
                'category': 'datasets',
                'detail_urls': dataset_urls
            }, None, None)]
        inps = self._crawl_repo_page_res
        kargs = {k: v for k, v in kargs.items() if k in ['threads', 'max_retries', 'screenshot_path']}
        crawler = HFDetailPageCrawler(**kargs)
        count = sum(len(inp.data['detail_urls']) for inp in inps if inp.data is not None)
        pbar = tqdm(total=count, desc="Crawling detail infos from HuggingFace...")
        res = []
        if save:
            writer = ModelDatasetJsonlineWriter(
                str(self.save_dir / "raw-models-info.jsonl"),
                str(self.save_dir / "raw-datasets-info.jsonl"),
                ['repo_org_mapper'], ['repo_org_mapper']
            )
        for inp in inps:
            crawler.parse_input(inp)
            for data in crawler.run():
                if data.error is not None:
                    self.error_writer.write(data.error)
                    error_f.flush()
                    pbar.update(1)
                    continue
                if save:
                    writer.parse_input(data)
                    res.append(next(writer.run()))
                else:
                    res.append(data)
                pbar.update(1)
        
        writer.close()
        pbar.close()
        self.error_writer.close()
        error_f.close()
        count = defaultdict(int)
        for data in res:
            if 'model_name' in data.data.keys():
                count['models'] += 1
            elif 'dataset_name' in data.data.keys():
                count['datasets'] += 1
        logger.info(f"Crawl detail page done. Total models: {count['models']}. Total datasets: {count['datasets']}")
        self._crawl_detail_page_res = res
        return self
    
    def _post_process(self, save, **kargs):
        error_f = self.error_f / 'post-process-error.jsonl'
        error_f = open(error_f, 'w')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Post Processing of HuggingFace data.")
        if not hasattr(self, "_crawl_detail_page_res"):
            logger.info("Missing the running result of the previous step (crawl_detail_page)")
            logger.info(f"Trying load required data from {self.save_dir}")
            org_links_reader = OrgLinksReader(sources=['HuggingFace'])
            models_reader = JsonlineReader(self.save_dir / 'raw-models-info.jsonl')
            datasets_reader = JsonlineReader(self.save_dir / 'raw-datasets-info.jsonl')
            repo_org_mapper = next(org_links_reader.run()).data['repo_org_mapper']
            self._crawl_detail_page_res = next(models_reader.run()).data.get('content')
            self._crawl_detail_page_res.extend(next(datasets_reader.run()).data.get['content'])
            for inp in self._crawl_detail_page_res:
                inp['repo_org_mapper'] = repo_org_mapper
        inps = self._crawl_detail_page_res
        kargs = {k: v for k, v in kargs.items() if k in [
            'dataset_info_path', 'model_info_path', 'ai_gen', 'ai_check',
            'buffer_size', 'max_retries'
        ]}
        processor = HFInfoProcessor(**kargs)
        res = []
        if save:
            writer = ModelDatasetJsonlineWriter(
                str(self.save_dir / 'processed-models-info.jsonl'),
                str(self.save_dir / 'processed-datasets-info.jsonl'),
            )
        for inp in inps:
            processor.parse_input(inp)
            for data in processor.run():
                if data.error is not None:
                    self.error_writer.write(data.error)
                    error_f.flush()
                    continue
                if save:
                    writer.parse_input(data)
                    res.append(next(writer.run()))
                else:
                    res.append(data)
        for data in processor.flush(update_infos=True):
            if data.error is not None:
                self.error_writer.write(data.error)
                error_f.flush()
                continue
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)

        if kargs.get('ai_check', False):
            model_check = {
                f'{data['repo']}/{data['model_name']}': data['downloads_last_month']
                for data in processor.models_check_buffer
            }
            dataset_check = {
                f'{data['repo']}/{data['dataset_name']}': data['downloads_last_month']
                for data in processor.datasets_check_buffer
            }
            back_writer = ModelDatasetJsonlineWriter(
                str(self.save_dir / "raw-models-info.jsonl"),
                str(self.save_dir / "raw-datasets-info.jsonl"),
                ['repo_org_mapper'], ['repo_org_mapper']
            )
            for inp in self._crawl_detail_page_res:
                if 'model_name' in inp.data.keys():
                    key = f'{inp.data['repo']}/{inp.data['model_name']}'
                    downloads_last_month = model_check.get(key, inp.data['downloads_last_month'])
                    inp.data['downloads_last_month'] = downloads_last_month
                elif 'dataset_name' in inp.data.keys():
                    key = f'{inp.data['repo']}/{inp.data['dataset_name']}'
                    downloads_last_month = dataset_check.get(key, inp.data['downloads_last_month'])
                    inp.data['downloads_last_month'] = downloads_last_month
                back_writer.parse_input(inp)
                next(back_writer.run())
            back_writer.close()
        
        writer.close()
        self.error_writer.close()
        error_f.close()
        count = defaultdict(int)
        for data in res:
            if 'model_name' in data.data.keys():
                count['models'] += 1
            elif 'dataset_name' in data.data.keys():
                count['datasets'] += 1
        logger.info(f"Post process done. Total models: {count['models']}. Total datasets: {count['datasets']}")
        self._post_process_res = res
        return self


class MSPipeline:
    
    def __init__(
        self,
        task_name: str,
        load_dir: str | None = None,
        save_dir: str | None = None,
        log_path: str | None = None,
    ):
        self.task_name = task_name
        self.crawl_date = datetime.now().strftime(r"%Y-%m-%d_%H-%M-%S")
        if load_dir:
            self.load_dir = Path(load_dir)
        else:
            self.load_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/ModelScope'
        if save_dir:
            self.save_dir = Path(save_dir)
        else:
            self.save_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/ModelScope'
        if log_path is None:
            log_path = Path(__file__).parents[2] / f'logs/{task_name}-{self.crawl_date}'/'running.log'
        else:
            log_path = Path(log_path)
        self.log_path = log_path
        self.log_path.parent.mkdir(exist_ok=True, parents=True)
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
        logger.add(log_path, level="DEBUG")
        self.error_f = log_path.parent
        
    def step_all(
        self,
        save: bool,
    ):
        return self._init_org_links(
            save
        )._crawl_repo_page(
            save        
        )._crawl_detail_page(
            save
        )._post_process(
            save
        )
        
    def step(
        self, 
        stage: Literal[
            'init_org_links',
            'crawl_repo_page',
            'crawl_detail_page',
            'post_process',
        ],
        save: bool,
        **kargs,
    ):
        match stage:
            case 'init_org_links':
                return self._init_org_links(save, **kargs)
            case 'crawl_repo_page':
                return self._crawl_repo_page(save, **kargs)
            case 'crawl_detail_page':
                return self._crawl_detail_page(save, **kargs)
            case 'post_process':
                return self._post_process(save, **kargs)
                
    def done(self):
        logger.success(f"MSPipeline {self.task_name} done.")
    
    def _init_org_links(self, save, **kargs):
        logger.info("Init org links of ModelScope")
        kargs = {k: v for k, v in kargs.items() if k in ['path', 'orgs']}
        kargs['sources'] = ['ModelScope']
        reader = OrgLinksReader(**kargs)
        reader.parse_input()
        res = next(reader.run())
        logger.info(f"Target orgs: {res.message['target_orgs']}")
        logger.info(f"Total links: {res.message['total_links']}")
        if save:
            save_path = self.save_dir / "org-links.jsonl"
            writer = JsonlineWriter(save_path)
            writer.parse_input(res)
            res = next(writer.run())
        writer.close()
        self._init_org_links_res = res
        return self
    
    def _crawl_repo_page(self, save, **kargs):
        error_f = self.error_f / 'org-links.jsonl'
        error_f = open(error_f, 'a')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Crawl repo page of ModelScope")
        if not hasattr(self, "_init_org_links_res"):
            logger.info("Missing the running result of the previous step (init_org_links)")
            logger.info(f"Trying load required data from {self.load_dir}")
            reader = JsonlineReader(self.load_dir/'org-links.jsonl')
            error_list = next(reader.run()).data.get('content')
            self._init_org_links_res = PipelineData({
                "ModelScope": [err['repo_link'] for err in error_list],
                "target_sources": ["ModelScope"],
            }, None, None)
        inp = self._init_org_links_res
        kargs = {k: v for k, v in kargs.items() if k in ['category', 'threads', 'max_retries']}
        crawler = MSRepoPageCrawler(**kargs)
        crawler.parse_input(inp)
        count = len(crawler.input['link-category'])
        pbar = tqdm(total=count, desc="Crawling repo infos from ModelScope...")
        res = []
        if save:
            save_path = self.save_dir / "repo-page.jsonl"
            writer = JsonlineWriter(save_path, drop_keys=['repo_org_mapper'])
        for data in crawler.run():
            if data.error is not None:
                self.error_writer.write(data.error)
                error_f.flush()
                pbar.update(1)
                continue
            pbar.write(f"{data.message['repo']} modelscope has {data.message['total_links']} {data.message['category']}.")
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)
            pbar.update(1)
        
        writer.close()
        pbar.close()
        self.error_writer.close()
        error_f.close()
        self._crawl_repo_page_res = res
        return self

    def _crawl_detail_page(self, save, **kargs):
        error_f = self.error_f / 'repo-page.jsonl'
        error_f = open(error_f, 'a')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Crawl detail page of ModelScope")
        if not hasattr(self, "_crawl_repo_page_res"):
            logger.info("Missing the running result of the previous step (crawl_repo_page)")
            logger.info(f"Trying load required data from {self.load_dir}")
            reader = JsonlineReader(self.load_dir/'repo-page.jsonl')
            error_list = next(reader.run()).data.get('content')
            model_urls = []
            dataset_urls = []
            for err in error_list:
                if err['category'] == 'models':
                    model_urls.append(err['detail_link'])
                elif err['category'] == 'datasets':
                    dataset_urls.append(err['detail_link'])
            self._crawl_repo_page_res = [PipelineData({
                'category': 'models',
                'detail_urls': model_urls
            }, None, None), PipelineData({
                'category': 'datasets',
                'detail_urls': dataset_urls
            }, None, None)]
        inps = self._crawl_repo_page_res
        kargs = {k: v for k, v in kargs.items() if k in ['threads', 'max_retries', 'screenshot_path']}
        crawler = MSDetailPageCrawler(**kargs)
        count = sum(len(inp.data['detail_urls']) for inp in inps if inp.data is not None)
        pbar = tqdm(total=count, desc="Crawling detail infos from ModelScope...")
        res = []
        if save:
            writer = ModelDatasetJsonlineWriter(
                str(self.save_dir / "raw-models-info.jsonl"),
                str(self.save_dir / "raw-datasets-info.jsonl"),
                ['repo_org_mapper'], ['repo_org_mapper']
            )
        for inp in inps:
            crawler.parse_input(inp)
            for data in crawler.run():
                if data.error is not None:
                    self.error_writer.write(data.error)
                    error_f.flush()
                    pbar.update(1)
                    continue
                if save:
                    writer.parse_input(data)
                    res.append(next(writer.run()))
                else:
                    res.append(data)
                pbar.update(1)
        
        writer.close()
        pbar.close()
        self.error_writer.close()
        error_f.close()
        count = defaultdict(int)
        for data in res:
            if 'model_name' in data.data.keys():
                count['models'] += 1
            elif 'dataset_name' in data.data.keys():
                count['datasets'] += 1
        logger.info(f"Crawl detail page done. Total models: {count['models']}. Total datasets: {count['datasets']}")
        self._crawl_detail_page_res = res
        return self
    
    def _post_process(self, save, **kargs):
        error_f = self.error_f / 'post-process-error.jsonl'
        error_f = open(error_f, 'w')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Post Processing of ModelScope data.")
        if not hasattr(self, "_crawl_detail_page_res"):
            logger.info("Missing the running result of the previous step (crawl_detail_page)")
            logger.info(f"Trying load required data from {self.save_dir}")
            org_links_reader = OrgLinksReader(sources=['ModelScope'])
            models_reader = JsonlineReader(self.save_dir / 'raw-models-info.jsonl')
            datasets_reader = JsonlineReader(self.save_dir / 'raw-datasets-info.jsonl')
            repo_org_mapper = next(org_links_reader.run()).data['repo_org_mapper']
            self._crawl_detail_page_res = next(models_reader.run()).data.get('content')
            self._crawl_detail_page_res.extend(next(datasets_reader.run()).data.get['content'])
            for inp in self._crawl_detail_page_res:
                inp['repo_org_mapper'] = repo_org_mapper
        inps = self._crawl_detail_page_res
        kargs = {k: v for k, v in kargs.items() if k in [
            'dataset_info_path', 'model_info_path', 'ai_gen', 'ai_check',
            'buffer_size', 'max_retries', 'history_data_path'
        ]}
        processor = MSInfoProcessor(**kargs)
        res = []
        if save:
            writer = ModelDatasetJsonlineWriter(
                str(self.save_dir / 'processed-models-info.jsonl'),
                str(self.save_dir / 'processed-datasets-info.jsonl'),
            )
        for inp in inps:
            processor.parse_input(inp)
            for data in processor.run():
                if data.error is not None:
                    self.error_writer.write(data.error)
                    error_f.flush()
                    continue
                if save:
                    writer.parse_input(data)
                    res.append(next(writer.run()))
                else:
                    res.append(data)
        for data in processor.flush(update_infos=True):
            if data.error is not None:
                self.error_writer.write(data.error)
                error_f.flush()
                continue
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)
                
        if kargs.get('ai_check', False):
            model_check = {
                f'{data['repo']}/{data['model_name']}': data['total_downloads']
                for data in processor.models_check_buffer
            }
            dataset_check = {
                f'{data['repo']}/{data['dataset_name']}': data['total_downloads']
                for data in processor.datasets_check_buffer
            }
            back_writer = ModelDatasetJsonlineWriter(
                str(self.save_dir / "raw-models-info.jsonl"),
                str(self.save_dir / "raw-datasets-info.jsonl"),
                ['repo_org_mapper'], ['repo_org_mapper']
            )
            for inp in self._crawl_detail_page_res:
                if 'model_name' in inp.data.keys():
                    key = f'{inp.data['repo']}/{inp.data['model_name']}'
                    downloads = model_check.get(key, inp.data['total_downloads'])
                    inp.data['total_downloads'] = downloads
                elif 'dataset_name' in inp.data.keys():
                    key = f'{inp.data['repo']}/{inp.data['dataset_name']}'
                    downloads = dataset_check.get(key, inp.data['total_downloads'])
                    inp.data['total_downloads'] = downloads
                back_writer.parse_input(inp)
                next(back_writer.run())
            back_writer.close()
        
        writer.close()
        self.error_writer.close()
        error_f.close()
        count = defaultdict(int)
        for data in res:
            if 'model_name' in data.data.keys():
                count['models'] += 1
            elif 'dataset_name' in data.data.keys():
                count['datasets'] += 1
        logger.info(f"Post process done. Total models: {count['models']}. Total datasets: {count['datasets']}")
        self._post_process_res = res
        return self


class OpenDataLabPipeline:
    
    def __init__(
        self,
        task_name: str,
        load_dir: str | None = None,
        save_dir: str | None = None,
        log_path: str | None = None,
    ):
        self.task_name = task_name
        self.crawl_date = datetime.now().strftime(r"%Y-%m-%d_%H-%M-%S")
        if load_dir:
            self.load_dir = Path(load_dir)
        else:
            self.load_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/OpenDataLab'
        if save_dir:
            self.save_dir = Path(save_dir)
        else:
            self.save_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/OpenDataLab'
        if log_path is None:
            log_path = Path(__file__).parents[2] / f"logs/{task_name}-{self.crawl_date}/running.log"
        else:
            log_path = Path(log_path)
        self.log_path = log_path
        self.log_path.parent.mkdir(exist_ok=True, parents=True)
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
        logger.add(log_path, level="DEBUG")
        self.error_f = log_path.parent
        
    def step(
        self,
        stage: Literal[
            'init_org_links',
            'crawl_repo_page',
            'post_process',
        ],
        save: bool,
        **kargs,
    ):
        match stage:
            case 'init_org_links':
                return self._init_org_links(save, **kargs)
            case 'crawl_repo_page':
                return self._crawl_repo_page(save, **kargs)
            case 'post_process':
                return self._post_process(save, **kargs)

    def done(self):
        logger.success(f"OpenDataLabPipeline {self.task_name} done.")
        
    def _init_org_links(self, save, **kargs):
        logger.info("Init org links of OpenDataLab")
        kargs = {k: v for k, v in kargs.items() if k in ['path', 'orgs']}
        kargs['sources'] = ['OpenDataLab']
        reader = OrgLinksReader(**kargs)
        reader.parse_input()
        res = next(reader.run())
        logger.info(f"Target orgs: {res.message['target_orgs']}")
        logger.info(f"Total links: {res.message['total_links']}")
        if save:
            save_path = self.save_dir / "org-links.jsonl"
            writer = JsonlineWriter(save_path)
            writer.parse_input(res)
            res = next(writer.run())
        writer.close()
        self._init_org_links_res = res
        return self
    
    def _crawl_repo_page(self, save, **kargs):
        logger.info("Crawl OpenDataLab page")
        if not hasattr(self, "_init_org_links_res"):
            raise RuntimeError("Missing the running result of the previous step (init_org_links)")
        inp = self._init_org_links_res
        kargs = {k: v for k, v in kargs.items() if k in ['threads', 'max_retries']}
        crawler = OpenDataLabCrawler(**kargs)
        crawler.parse_input(inp)
        count = len(crawler.input['links'])
        pbar = tqdm(total=count, desc="Crawling OpenDataLab infos...")
        res = []
        if save:
            save_path = self.save_dir / "raw-datasets-info.jsonl"
            writer = JsonlineWriter(save_path)
        for data in crawler.run():
            if data.error is not None:
                raise RuntimeError("Error crawling OpenDataLab page.")
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)
            pbar.update(1)
            
        writer.close()
        pbar.close()
        self._crawl_repo_page_res = res
        
    def _post_process(self, save, **kargs):
        error_f = self.error_f / 'post-process-error.jsonl'
        error_f = open(error_f, 'w')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Post Processing of OpenDataLab data.")
        if not hasattr(self, "_crawl_repo_page_res"):
            logger.info("Missing the running result of the previous step (crawl_repo_page)")
            logger.info(f"Trying load required data from {self.save_dir}")
            datasets_reader = JsonlineReader(self.save_dir / 'raw-datasets-info.jsonl')
            self._crawl_repo_page_res = next(datasets_reader.run()).data.get['content']
        inps = self._crawl_repo_page_res
        kargs = {k: v for k, v in kargs.items() if k in [
            'dataset_info_path', 'history_data_path', 'ai_gen',
            'buffer_size', 'max_retries'
        ]}
        processor = OpenDataLabInfoProcessor(**kargs)
        res = []
        if save:
            writer = JsonlineWriter(str(self.save_dir / 'processed-datasets-info.jsonl'))
        for inp in inps:
            processor.parse_input(inp)
            for data in processor.run():
                if data.error is not None:
                    self.error_writer.write(data.error)
                    error_f.flush()
                    continue
                if save:
                    writer.parse_input(data)
                    res.append(next(writer.run()))
                else:
                    res.append(data)
        for data in processor.flush(update_infos=True):
            if data.error is not None:
                self.error_writer.write(data.error)
                error_f.flush()
                continue
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)
        
        writer.close()
        self.error_writer.close()
        error_f.close()
        count = len(res)
        logger.info(f"Post process done. Total datasets: {count}")
        self._post_process_res = res
        return self


class BAAIDataPipeline:
    
    def __init__(
        self,
        task_name: str,
        load_dir: str | None = None,
        save_dir: str | None = None,
        log_path: str | None = None,
    ):
        self.task_name = task_name
        self.crawl_date = datetime.now().strftime(r"%Y-%m-%d_%H-%M-%S")
        if load_dir:
            self.load_dir = Path(load_dir)
        else:
            self.load_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/BAAIData'
        if save_dir:
            self.save_dir = Path(save_dir)
        else:
            self.save_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/BAAIData'
        if log_path is None:
            log_path = Path(__file__).parents[2] / f"logs/{task_name}-{self.crawl_date}/running.log"
        else:
            log_path = Path(log_path)
        self.log_path = log_path
        self.log_path.parent.mkdir(exist_ok=True, parents=True)
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
        logger.add(log_path, level="DEBUG")
        self.error_f = log_path.parent
        
    def step(
        self,
        stage: Literal[
            'init_org_links',
            'crawl_repo_page',
            'post_process',
        ],
        save: bool,
        **kargs,
    ):
        match stage:
            case 'init_org_links':
                return self._init_org_links(save, **kargs)
            case 'crawl_repo_page':
                return self._crawl_repo_page(save, **kargs)
            case 'post_process':
                return self._post_process(save, **kargs)

    def done(self):
        logger.success(f"BAAIDataPipeline {self.task_name} done.")
        
    def _init_org_links(self, save, **kargs):
        logger.info("Init org links of BAAIData")
        kargs = {k: v for k, v in kargs.items() if k in ['path']}
        kargs['sources'] = ['BAAIData']
        reader = OrgLinksReader(**kargs)
        reader.parse_input()
        res = next(reader.run())
        logger.info(f"Target orgs: {res.message['target_orgs']}")
        logger.info(f"Total links: {res.message['total_links']}")
        if save:
            save_path = self.save_dir / "org-links.jsonl"
            writer = JsonlineWriter(save_path)
            writer.parse_input(res)
            res = next(writer.run())
        writer.close()
        self._init_org_links_res = res
        return self
    
    def _crawl_repo_page(self, save, **kargs):
        logger.info("Crawl OpenDataLab page")
        if not hasattr(self, "_init_org_links_res"):
            raise RuntimeError("Missing the running result of the previous step (init_org_links)")
        inp = self._init_org_links_res
        kargs = {k: v for k, v in kargs.items() if k in ['max_retries']}
        crawler = BAAIDatasetsCrawler(**kargs)
        crawler.parse_input(inp)
        res = []
        if save:
            save_path = self.save_dir / "raw-datasets-info.jsonl"
            writer = JsonlineWriter(save_path)
        for data in crawler.run():
            if data.error is not None:
                raise RuntimeError("Error crawling BAAIData.")
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)
            
        writer.close()
        self._crawl_repo_page_res = res
        
    def _post_process(self, save, **kargs):
        error_f = self.error_f / 'post-process-error.jsonl'
        error_f = open(error_f, 'w')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Post Processing of BAAIData data.")
        if not hasattr(self, "_crawl_repo_page_res"):
            logger.info("Missing the running result of the previous step (crawl_repo_page)")
            logger.info(f"Trying load required data from {self.save_dir}")
            datasets_reader = JsonlineReader(self.save_dir / 'raw-datasets-info.jsonl')
            self._crawl_repo_page_res = next(datasets_reader.run()).data.get['content']
        inps = self._crawl_repo_page_res
        kargs = {k: v for k, v in kargs.items() if k in [
            'dataset_info_path', 'history_data_path', 'ai_gen',
            'buffer_size', 'max_retries'
        ]}
        processor = BAAIDataInfoProcessor(**kargs)
        res = []
        if save:
            writer = JsonlineWriter(str(self.save_dir / 'processed-datasets-info.jsonl'))
        for inp in inps:
            processor.parse_input(inp)
            for data in processor.run():
                if data.error is not None:
                    self.error_writer.write(data.error)
                    error_f.flush()
                    continue
                if save:
                    writer.parse_input(data)
                    res.append(next(writer.run()))
                else:
                    res.append(data)
        for data in processor.flush(update_infos=True):
            if data.error is not None:
                self.error_writer.write(data.error)
                error_f.flush()
                continue
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)
        
        writer.close()
        self.error_writer.close()
        error_f.close()
        count = len(res)
        logger.info(f"Post process done. Total datasets: {count}")
        self._post_process_res = res
        return self


class MergeAndRankingPipeline:
    
    def __init__(
        self,
        data_dir: str | None,
        log_path: str | None,
    ):
        self.now = datetime.now().strftime(r"%Y-%m-%d_%H-%M-%S")
        date = str(datetime.today().date())
        if data_dir:
            self.data_dir = Path(data_dir)
        else:
            self.data_dir = Path(__file__).parents[2] / f'data/{date}'
        if log_path is None:
            log_path = Path(__file__).parents[2] / f"logs/ranking-{self.now}/running.log"
        else:
            log_path = Path(log_path)
        self.log_path = log_path
        self.log_path.parent.mkdir(exist_ok=True, parents=True)
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
        logger.add(log_path, level="DEBUG")
        
    def step(
        self,
        stage: Literal[
            "merge_models",
            "merge_datasets",
            "ranking",
        ],
        save: bool = True,
        **kargs,
    ):
        match stage:
            case 'merge_models':
                return self._merge_models(save, **kargs)
            case 'merge_datasets':
                return self._merge_datasets(save, **kargs)
            case 'ranking':
                return self._ranking(save, **kargs)
    
    def done(self):
        logger.success("Merge and ranking done.")
        
    def _merge_models(self, save, **kargs):
        logger.info("Merge models")
        buffer = defaultdict(list)
        for p in self.data_dir.iterdir():
            data_path = p / 'processed-models-info.jsonl'
            if not data_path.exists():
                continue
            with jsonlines.open(data_path, 'r') as reader:
                for data in reader:
                    key = f"{data['org']}/{data['model_name']}"
                    buffer[key].append(data)
        if len(buffer) == 0:
            raise RuntimeError("No processed data found.")
        save_path = self.data_dir / 'merged-models-info.jsonl'
        with jsonlines.open(save_path, 'w') as writer:
            for _, models in buffer.items():
                data = {
                    "org": models[0]['org'],
                    "repo": models[0]['repo'],
                    "model_name": models[0]['model_name'],
                    "modality": models[0]['modality'],
                    "downloads_last_month": sum(model['downloads_last_month']
                                                for model in models
                                                if model['downloads_last_month'] > 0),
                    "likes": sum(model['likes'] for model in models),
                    "community": sum(model['community'] for model in models
                                        if 'community' in model),
                    "descendants": sum(model['descendants'] for model in models
                                        if 'descendants' in model),
                    "date_crawl": models[0]['date_crawl'],
                } # TODO Currently missing the date_last_crawl and date_enter_db fields
                writer.write(data)
        logger.info(f"Total model records: {len(buffer)}")
        
    def _merge_datasets(self, save, **kargs):
        logger.info("Merge datasets")
        buffer = defaultdict(list)
        for p in self.data_dir.iterdir():
            data_path = p / 'processed-datasets-info.jsonl'
            if not data_path.exists():
                continue
            with jsonlines.open(data_path, 'r') as reader:
                for data in reader:
                    key = f"{data['org']}/{data['dataset_name']}"
                    buffer[key].append(data)
        if len(buffer) == 0:
            raise RuntimeError("No processed data found.")
        save_path = self.data_dir / 'merged-datasets-info.jsonl'
        with jsonlines.open(save_path, 'w') as writer:
            for _, datasets in buffer.items():
                data = {
                    "org": datasets[0]['org'],
                    "repo": datasets[0]['repo'],
                    "dataset_name": datasets[0]['dataset_name'],
                    "modality": datasets[0]['modality'],
                    "lifecircle": datasets[0]['lifecircle'],
                    "downloads_last_month": sum(dataset['downloads_last_month']
                                                for dataset in datasets
                                                if dataset['downloads_last_month'] > 0),
                    "likes": sum(dataset['likes'] for dataset in datasets),
                    "community": sum(dataset['community'] for dataset in datasets
                                        if 'community' in dataset),
                    "dataset_usage": sum(dataset['dataset_usage'] for dataset in datasets
                                            if 'dataset_usage' in dataset),
                    "date_crawl": datasets[0]['date_crawl'],
                } # TODO Currently missing the date_last_crawl and date_enter_db fields
                writer.write(data)
        logger.info(f"Total datasets records: {len(buffer)}")
    
    def _ranking(self, save, **kargs):
        pass
