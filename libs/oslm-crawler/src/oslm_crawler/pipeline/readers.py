import json
from .base import PipelineStep, PipelineResult, PipelineData
from pathlib import Path
from collections import defaultdict
from typing import TypedDict


class OrgLinksReader(PipelineStep):
    
    ptype = "📖 READER"
    desired_keys = []
    
    def __init__(
        self, 
        path: str | Path | None = None, 
        orgs: list[str] | None = None,
        sources: list[str] | None = None,
    ):
        if isinstance(path, str):
            path = Path(path)
        elif path is None:
            script_path = Path(__file__)
            path = script_path.parents[3] / "config/org-links.json"
        self.input = path
        self.orgs = orgs
        self.sources = sources
        
    def parse_input(self, input_data: PipelineData | None = None):
        if input_data is None:
            self.data = {}
            return 
        self.data = input_data.data.copy()
        
    def run(self) -> PipelineResult:
        with self.input.open('r', encoding='utf-8') as f:
            config: dict[str, dict[str, list[str]]] = json.load(f)
            
        try:
            assert isinstance(config, dict), 'config must have a structure like dict[str, dict[str, list[str]]]'
            all_sources = set(k2 for v1 in config.values() for k2 in v1.keys())
            assert len(all_sources) > 0, 'No sources'
            assert self.sources is None or set(self.sources).issubset(all_sources)
            target_sources = set(self.sources or all_sources)
            target_orgs = set(
                k1 for k1, v1 in config.items() if set(v1.keys()).intersection(target_sources))
            if self.orgs:
                target_orgs = set(self.orgs) & target_orgs
            target_config = defaultdict(dict)
            for k1, v1 in config.items():
                if k1 not in target_orgs:
                    continue
                for k2, v2 in v1.items():
                    if k2 not in target_sources:
                        continue
                    target_config[k1][k2] = v2
            assert len(target_config) > 0, 'No config found that meets the orgs and sources conditions'
            repo_org_mapper = {}
            for org, v in target_config.items():
                for src, links in v.items():
                    if src in ['Hugging Face', 'ModelScope']:
                        for link in links:
                            repo = link.rstrip('/').split('/')[-1]
                            if repo in repo_org_mapper:
                                assert repo_org_mapper[repo] == org
                            else:
                                repo_org_mapper[repo] = org
            target_links = defaultdict(list)
            for _, v1 in target_config.items():
                for src, links in v1.items():
                    target_links[src].extend(links)
            data = target_links.copy()
            total_links = sum(len(v) for v in target_links.values())
            data.update({
                "target_sources": target_sources,
                "repo_org_mapper": repo_org_mapper.copy(),
            })
            data.update(self.data)
            message = {
                "target_sources": target_sources,
                "target_orgs": target_orgs,
                "total_links": total_links,
            }
            
            res = PipelineData(data, message, None)
        except Exception as e:
            import traceback
            error_msg = traceback.format_exc()
            res = PipelineData(None, None, {"type": type(e), "details": error_msg})
        
        yield res
        

class JsonlineReader(PipelineStep):
    
    ptype = "📖 READER"
    
    def __init__(
        self,
    ):
        pass