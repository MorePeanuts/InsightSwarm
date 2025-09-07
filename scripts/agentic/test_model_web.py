import yaml
import os
from langchain.chat_models import init_chat_model

SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))
ROOT_PATH = os.path.dirname(os.path.dirname(SCRIPT_PATH))

with open(os.path.join(ROOT_PATH, 'config/env.yaml'), 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)
    if "OPENAI_API_KEY" not in os.environ:
        os.environ["OPENAI_API_KEY"] = config["OPENAI"][0]["OPENAI_API_KEY"]
    if "OPENAI_API_BASE" not in os.environ:
        os.environ["OPENAI_API_BASE"] = config["OPENAI"][0]["OPENAI_API_BASE"]
        
llm = init_chat_model("gpt-5", model_provider="openai")

msg = llm.invoke("请总结以下网页中的内容，如果你无法访问网页，请回复无法访问：\nhttps://huggingface.co/nvidia/GR00T-N1-2B")

print(msg)