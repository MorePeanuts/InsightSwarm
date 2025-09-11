from collections import defaultdict
import jsonlines
import sys
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
            models_reader = JsonlineReader(self.save_dir / 'raw-models-info.jsonl')
            datasets_reader = JsonlineReader(self.save_dir / 'raw-datasets-info.jsonl')
            self._crawl_detail_page_res = next(models_reader.run()).data.get('content')
            self._crawl_detail_page_res.extend(next(datasets_reader.run()).data.get['content'])
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
        self.error_f.mkdir(parents=True, exist_ok=True)
        
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
            models_reader = JsonlineReader(self.save_dir / 'raw-models-info.jsonl')
            datasets_reader = JsonlineReader(self.save_dir / 'raw-datasets-info.jsonl')
            self._crawl_detail_page_res = next(models_reader.run()).data.get('content')
            self._crawl_detail_page_res.extend(next(datasets_reader.run()).data.get['content'])
        inps = self._crawl_detail_page_res
        kargs = {k: v for k, v in kargs.items() if k in [
            'dataset_info_path', 'model_info_path', 'ai_gen', 'ai_check',
            'buffer_size', 'max_retries'
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
        save_dir: str | None = None,
        log_path: str | None = None,
    ):
        self.task_name = task_name
        self.crawl_date = datetime.now().strftime(r"%Y-%m-%d_%H-%M-%S")
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
        logger.success(f"OpenDataLabPipeline {self.task_name} doen.")
        
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
        pass
        


class BAAIDataPipeline:
    pass


class MergePipeline:
    pass


class RankingPipeline:
    pass

