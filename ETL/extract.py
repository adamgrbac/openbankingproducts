import json
import os
import pandas as pd
from datetime import date
from tqdm import tqdm

def extract_raw(source_dir):
    data = []
    for filename in tqdm(os.listdir(source_dir), ascii=True, desc="Extract Raw Data"):
        with open(f"{source_dir}/{filename}") as f:
            try:
                data.append(json.load(f))
            except Exception as e:
                print("\nERROR! Can't load file!")
                print(f"Filename: {filename}")
    return data
    
def extract_model(model_entity, source_dirs):
    dfs = []
    for source in tqdm(source_dirs, ascii=True, desc="Extract Model Data"):
        dfs.append(pd.read_csv(f"./data/model/{source}/{model_entity}.csv"))
    return pd.concat(dfs)