import json
import streamlit as st
import pandas as pd
from pathlib import Path

def set_multi_level_columns(df, mapping):
    new_columns = []
    for col in df.columns:
        level1, level2 = mapping.get(col, ('合计', col))
        new_columns.append((level1, level2))
    
    df.columns = pd.MultiIndex.from_tuples(new_columns)
    return df

root_path = Path(__file__).parents[1]
key_path = Path(__file__).parents[1] / 'config/ranking-key-zh.json'
with key_path.open('r', encoding='utf-8') as f:
    mapping = json.load(f)['Data']

choices = []

for path in (root_path / 'data').glob("????-??-??"):
    overall_rank_path = path / 'data-rank.csv'
    if overall_rank_path.exists():
        choices.append(path.name)
        
option = st.selectbox(
    "Select date",
    list(sorted(choices, reverse=True))
)
table_type = st.selectbox(
    "Select table type",
    ['monthly rank', 'accumulated rank', 'monthly metrics', 'accumulated metrics', 'delta metrics']
)

match table_type:
    case 'monthly rank':
        cur_path = root_path / 'data' / option / 'data-rank.csv'
    case 'accumulated rank':
        cur_path = root_path / 'data' / option / 'data-rank.csv'
    case 'monthly metrics':
        cur_path = root_path / 'data' / option / 'data-summary.csv'
    case 'accumulated metrics':
        cur_path = root_path / 'data' / option / 'data-summary.csv'
    case 'delta metrics':
        cur_path = root_path / 'data' / option / 'data-summary-delta.csv'

data = pd.read_csv(cur_path, index_col='org')
data = set_multi_level_columns(data, mapping)
data