import json
from pathlib import Path
from typing import Literal
from .base import PipelineStep, PipelineResult, PipelineData
from ..database.record import HFModelRecord, HFDatasetRecord


class HFInfoProcessor(PipelineStep):
    
    ptype = "🚗 PROCESSOR"
    desired_keys = ["repo_org_mapper"]
    
    def __init__(
        self,
        dataset_info_path: str | None = None,
        model_info_path: str | None = None,
        ai_gen: bool = True,
        ai_check: bool = False
    ):
        self.ai_gen = ai_gen
        self.ai_check = ai_check
        if self.ai_gen:
            from ..ai.model_info_generator import gen_model_info
            from ..ai.dataset_info_generator import gen_dataset_info
        if self.ai_check:
            from ..ai.screenshot_checker import check_image_info

        curr_path = Path(__file__)
        if dataset_info_path:
            dataset_info_path = Path(dataset_info_path)
        else:
            dataset_info_path = curr_path.parents[3] / 'config/dataset-info.json'
        if model_info_path:
            model_info_path = Path(model_info_path)
        else:
            model_info_path = curr_path.parents[3] / 'config/model-info.json'
        self.model_info = self._init_info(model_info_path)
        self.dataset_info = self._init_info(dataset_info_path)
        
    def _init_info(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            info = json.load(f)
        return info
        
    def parse_input(self, input_data: PipelineData | None = None):
        self.desired_keys = [
            'repo', 'downloads_last_month', 'likes', 'community', 'date_crawl',
            'link', 'img_path', 'error_msg', 'metadata'
        ]
        if 'model_name' in input_data.data.keys():
            self.category = 'models'
            self.desired_keys += [
                'repo_org_mapper', 'model_name', 'descendants'
            ]
        elif 'dataset_name' in input_data.data.keys():
            self.category = 'datasets'
            self.desired_keys += [
                'repo_org_mapper', 'dataset_name', 'dataset_usage'
            ]
        else:
            raise KeyError('input_data.data must contains model_name or dataset_name.')
        self.data = input_data.data.copy()
        self.input = {}
        for k in self.desired_keys:
            if k not in self.data:
                raise KeyError(f"key '{k}' not found in input_data.data "
                               f"{list(input_data.data.keys())} of {self.__class__}")
            self.input[k] = self.data.pop(k)
            
    def run(self) -> PipelineResult:
        # TODO processor
        try:
            repo = self.input['repo']
            org = self.input['repo_org_mapper'][repo]
        except Exception:
            pass
            
            
            

class MSInfoProcessor(PipelineStep):
    
    ptype = "🚗 PROCESSOR"
    desired_keys = ["repo_org_mapper"]
    
    
class OpenDataLabInfoProcessor(PipelineStep):
    
    ptype = "🚗 PROCESSOR"
    desired_keys = []
    
    
class BAAIDataInfoProcessor(PipelineStep):
    
    ptype = "🚗 PROCESSOR"
    
    
class MultiSourceInfoMerge(PipelineStep):
    
    ptype = "🚗 PROCESSOR"
