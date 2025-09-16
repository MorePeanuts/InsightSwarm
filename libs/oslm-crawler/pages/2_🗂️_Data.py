import streamlit as st
import pandas as pd
from pathlib import Path

root_path = Path(__file__).parents[1]
choices = []

for path in (root_path / 'data').glob("????-??-??"):
    overall_rank_path = path / 'data-rank.csv'
    if overall_rank_path.exists():
        choices.append(path.name)
        
option = st.selectbox(
    "Select date",
    list(sorted(choices))
)
table_type = st.selectbox(
    "Select table type",
    ['monthly rank', 'accumulated rank', 'monthly metrics', 'accumulated metrics']
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

data = pd.read_csv(cur_path, index_col='org')
data