from typing import Literal, NamedTuple


class ModelRecord(NamedTuple):
    org: str
    repo: str
    model_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Protein', 'Vector', '3D', 'Embodied']
    downloads_last_month: int
    likes: int
    community: int
    descendants: int
    date_crawl: str
    date_enter_db: str
    

class DatasetRecord(NamedTuple):
    org: str
    repo: str
    dataset_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Embodied']
    lifecycle: Literal['Pre-training', 'Fine-tuning', 'Preference', 'Evaluation']
    downloads_last_month: int
    likes: int
    community: int
    dataset_usage: int
    date_crawl: str
    date_enter_db: str


class HFModelRecord(NamedTuple):
    org: str
    repo: str
    model_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Protein', 'Vector', '3D', 'Embodied']
    downloads_last_month: int
    likes: int
    community: int
    descendants: int
    date_crawl: str
    date_enter_db: str
    link: str
    img_path: str | None


class HFDatasetRecord(NamedTuple):
    org: str
    repo: str
    dataset_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Embodied']
    lifecycle: Literal['Pre-training', 'Fine-tuning', 'Preference', 'Evaluation']
    downloads_last_month: int
    likes: int
    community: int
    dataset_usage: int
    date_crawl: str
    date_enter_db: str
    link: str
    img_path: str | None


class MSModelRecord(NamedTuple):
    org: str
    repo: str
    model_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Protein', 'Vector', '3D', 'Embodied']
    downloads_last_month: int
    total_downloads: int
    likes: int
    community: int
    date_crawl: str
    date_enter_db: str
    link: str
    img_path: str | None


class MSDatasetRecord(NamedTuple):
    org: str
    repo: str
    dataset_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Embodied']
    lifecycle: Literal['Pre-training', 'Fine-tuning', 'Preference', 'Evaluation']
    downloads_last_month: int
    total_downloads: int
    likes: int
    community: int
    date_crawl: str
    date_enter_db: str
    link: str
    img_path: str | None

    
class OpenDataLabRecord(NamedTuple):
    org: str
    repo: str
    dataset_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Embodied']
    lifecycle: Literal['Pre-training', 'Fine-tuning', 'Preference', 'Evaluation']
    downloads_last_month: int
    total_downloads: int
    likes: int
    date_crawl: str
    date_enter_db: str
    link: str
    
    
class BAAIDataRecord(NamedTuple):
    org: str
    repo: str
    dataset_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Embodied']
    lifecycle: Literal['Pre-training', 'Fine-tuning', 'Preference', 'Evaluation']
    downloads_last_month: int
    total_downloads: int
    likes: int
    date_crawl: str
    date_enter_db: str
    link: str    
