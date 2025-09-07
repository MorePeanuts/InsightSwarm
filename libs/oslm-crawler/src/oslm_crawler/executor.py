from urllib3.exceptions import NotOpenSSLWarning
from .pipeline.base import PipelineStep
from typing import Callable


class PipelineExecutor:
    
    def __init__(
        self,
        pipeline: list[PipelineStep | Callable],
        logging_dir: str | None = None,    
        skip_completed: bool = True,
        debug_mode: bool = False,
    ):
        self.pipeline = pipeline
        self.logging_dir = logging_dir
        self.skip_completed = skip_completed
        self.debug_mode = debug_mode
        
    def run(self):
        pass
    
    # TODO Run asynchronously
    def arun(self):
        pass
    
    
PRELUDE_PIPELINE = {
    "huggingface-full": [
        
    ],
    "huggingface-models": [
        
    ],
    "huggingface-datasets": [
        
    ],
    "modelscope-full": [
        
    ],
    "modelscope-models": [
        
    ],
    "modelscope-datasets": [
        
    ],
    "full": [
        
    ],
    "datasets-only": [
        
    ],
    "models-only": [
        
    ],
}