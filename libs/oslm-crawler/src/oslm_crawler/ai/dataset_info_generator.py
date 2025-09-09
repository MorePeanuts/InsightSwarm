"""
Use the web access tool that comes with the grok-3-all model to determine the dataset modality through the repository link.
"""
import os
import yaml
from pathlib import Path
from langchain.chat_models import init_chat_model
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from pydantic import BaseModel, Field
from typing import Literal, Optional


SCRIPT_PATH = Path(__file__)
ROOT_PATH = SCRIPT_PATH.parents[5]
CONFIG_PATH = ROOT_PATH / 'config/env.yaml'
with CONFIG_PATH.open('r', encoding='utf-8') as f:
    config = yaml.safe_load(f)
    if "OPENAI_API_KEY" not in os.environ:
        os.environ["OPENAI_API_KEY"] = config["OPENAI"][0]["OPENAI_API_KEY"]
    if "OPENAI_API_BASE" not in os.environ:
        os.environ["OPENAI_API_BASE"] = config["OPENAI"][0]["OPENAI_API_BASE"]


class DatasetInfo(BaseModel):
    link: str = Field(description="The link of the dataset")
    modality: Optional[Literal["Language", "Speech", "Vision", "Multimodal", "Vector", "Protein", 
                               "3D", "Embodied"]] = Field(default=None, description="The modality of the dataset")
    lifecircle: Optional[Literal["Pre-training", "Fine-tuning", "Preference", "Evaluation"]] = Field(default=None, description="Which stage of model training/evaluation is the dataset used for")
    is_valid: Optional[bool] = Field(default=None, description="Is it a valid dataset that can be used for large model evaluation/training")
    
    
class DatasetInfoList(BaseModel):
    infos: list[DatasetInfo] = Field(description="The list of dataset information")


llm_web_search = init_chat_model("grok-3-all", model_provider="openai", temperature=0)
llm_json_parse = init_chat_model("gpt-5", model_provider="openai")

web_search_prompt = """\
You are an expert in machine learning datasets and their applications. Your task is to search all the following dataset repository links (HuggingFace or Modelscope), and judge based on the webpage information by following these steps:

1.  First, determine if the link points to a valid dataset. A dataset is considered **valid** if its repository contains accessible data files and appears genuinely intended for model training or evaluation. It is **not valid** if the repository is empty, a placeholder, used only for testing, or otherwise lacks usable data.

2.  If and only if the dataset is valid, identify its modality. Possible modalities are:
    -   Language: Datasets consisting of text (e.g., books, articles, code, instruction-response pairs).
    -   Speech: Datasets containing audio of spoken language.
    -   Vision: Datasets of images or videos.
    -   Multimodal: Datasets combining two or more modalities (e.g., images with text captions, video with audio).
    -   Embodied: Datasets for robotics or embodied AI, often involving sequences of actions, observations, and rewards.

3.  If and only if the dataset is valid, identify which stage in the large model lifecycle it is primarily used for. Possible stages are:
    -   Pre-training: Very large, general-purpose datasets, often with raw or weakly labeled data, used to train foundation models from scratch (e.g., C4, The Pile).
    -   Fine-tuning: Smaller, high-quality, task-specific datasets, often structured as instructions and responses, used to adapt a pre-trained model to a specific capability (e.g., Alpaca, Dolly).
    -   Preference: Datasets used for alignment techniques like RLHF or DPO, typically containing prompts and multiple responses with human or AI-judged rankings/preferences (e.g., Anthropic HH-RLHF).
    -   Evaluation: Benchmark datasets with ground-truth labels used to measure model performance on specific tasks (e.g., MMLU, Hellaswag, HumanEval).

Instructions:
If you do not have the ability to access the network, state that you cannot access the network.
For each link, you must first state whether it is a valid dataset.
If a dataset is judged as not valid, you should clearly state so and do not need to provide its modality or lifecircle.
If you cannot determine any piece of information (validity, modality, or lifecircle), you should clearly point it out rather than guessing a result.
Keep the correspondence between each link and the conclusions you give.

Dataset links:
{dataset_links}
"""

json_parse_prompt = """\
You are an expert skilled at extracting effective information. Your task is to extract information based on a summary text from web searches, following the format of the `DatasetInfoList` class. This summary text describes information from a list of HuggingFace or Modelscope dataset repositories, including whether the dataset is valid, its modality, its use in the model lifecircle, and the web link.

Follow these critical rules during extraction:
1. First, determine the value for the `is_valid` field (true or false).
2. Crucially, if `is_valid` is `False`, then both `modality` and `lifecircle` must be set to `None`, regardless of any other information present in the text.
3. If the text states that it cannot determine any of the fields (`is_valid`, `modality`, or `lifecircle`), then you should set that specific field to `None`.

The `modality` must be one of the following: Language, Speech, Vision, Multimodal, Embodied.
The `lifecircle` must be one of the following: Pre-training, Fine-tuning, Preference, Evaluation.

Here is the summary text:
{web_search_result}
"""

web_search_prompt_template = ChatPromptTemplate.from_template(web_search_prompt)
json_parse_prompt_template = ChatPromptTemplate.from_template(json_parse_prompt)

json_parser = llm_json_parse.with_structured_output(DatasetInfoList, include_raw=True)
chain = (
    web_search_prompt_template 
    | llm_web_search 
    | StrOutputParser()
    | {"web_search_result": lambda x: x}
    | json_parse_prompt_template 
    | json_parser
)

def gen_dataset_info(urls: list[str]):
    result = chain.invoke({"dataset_links": urls})
    if result['parsing_error'] is None:
        return result['parsed'].infos
    else:
        print("Parsing error:", result['parsing_error'])
        return [DatasetInfo(link=url, modality=None, lifecircle=None) for url in urls]


if __name__ == "__main__":
    from pprint import pprint
    urls = [
        "https://huggingface.co/datasets/zai-org/AgentInstruct",
        "https://huggingface.co/datasets/allenai/reward-bench-2-results",
        "https://huggingface.co/datasets/Skywork/SkyPile-150B",
        "https://huggingface.co/datasets/HuggingFaceTB/smoltalk2",
        "https://huggingface.co/datasets/OmniGen2/X2I2",
        "https://huggingface.co/datasets/microsoft/rStar-Coder",
        "https://huggingface.co/datasets/OpenGVLab/Doc-750K",
        "https://huggingface.co/datasets/BAAI/CI-VID",
        "https://huggingface.co/datasets/allenai/layout_distribution_shift",
        "https://huggingface.co/datasets/facebook/community-alignment-dataset",
        "https://huggingface.co/datasets/nvidia/Nemotron-Post-Training-Dataset-v1",
        "https://huggingface.co/datasets/zai-org/LongBench",
        "https://modelscope.cn/datasets/ZhipuAI/VisionRewardDB-Image-regression",
        "https://huggingface.co/datasets/allenai/PRISM",
        "https://huggingface.co/datasets/microsoft/mocapact-data",
        "https://huggingface.co/datasets/BAAI/ShareRobot"
    ]
    datasets = gen_dataset_info(urls)
    pprint(datasets)
