import jsonlines
import traceback
from .base import PipelineStep, PipelineResult, PipelineData
from pathlib import Path
from loguru import logger


class JsonlineWriter(PipelineStep):
    
    ptype = "✍️ WRITER"
    required_keys = []
    
    def __init__(
        self,
        path: Path,
        required_keys: list[str] | None = None,
        drop_keys: list[str] | None = None,
    ):
        self.required_keys = required_keys
        if drop_keys:
            self.drop_keys = drop_keys
        else:
            self.drop_keys = []
        self.path = path
        assert self.path.suffix == '.jsonl', 'The path must end with a filename that has a `.jsonl` suffix.'
        self.path.parent.mkdir(exist_ok=True)
        self.path.touch()
        self.f = open(self.path, 'w')
        self.writer = jsonlines.Writer(self.f)
    
    def parse_input(self, input_data: PipelineData | None = None):
        if self.required_keys is None:
            self.required_keys = list(input_data.data.keys())
        self.required_keys = [x for x in self.required_keys if x not in self.drop_keys]
        self.data = input_data.data.copy()
        desired_data = {}
        for k in self.required_keys:
            if k not in self.data:
                raise KeyError(f"key '{k}' not found in input_data.data "
                               f"{list(input_data.data.keys())} of {self.__class__}")
            desired_data[k] = self.data[k]
        self.input = desired_data
        
    def run(self) -> PipelineResult:
        try:
            self.writer.write(self.input)
            self.f.flush()
            yield PipelineData(self.data, None, None)
        except Exception:
            logger.exception(f"Error write jsonline data:\n {self.input}")
            yield PipelineData(None, None, {
                'input': self.input,
                'error_msg': traceback.format_exc(),
            })
        
    def close(self) -> bool:
        self.writer.close()
        self.f.close()
    

# TODO database writer    
class DBWriter(PipelineStep):
    
    ptype = "✍️ WRITER"
    
    def __init__(
        self,
        conn,
    ):
        raise NotImplementedError
    
    def parse_input(self, input_data: PipelineData | None = None):
        raise NotImplementedError
    
    def run(self) -> PipelineResult:
        yield PipelineData(None, str(NotImplementedError()), None)
