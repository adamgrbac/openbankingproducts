import requests
import json
import os
import pandas as pd
from datetime import date
from tqdm import tqdm

def load_raw(source, src_url, src_date):
    current_date = src_date.strftime('%Y%m%d')
    # Open Banking API required headers
    # x-v: Requesting Open Banking API Version
    # x-v-min: Minimum Acceptable Open Banking API Versiom
    product_headers = {"x-v":"3", "x-v-min":"2"}
    data = []
    url = src_url
    
    while url and url != "":
        res = requests.get(url, headers = product_headers)
        if res.status_code == 200:
            try:
                data.extend(res.json()["data"]["products"])
                url = res.json()["links"].get("next","")
            except Exception as e:
                print("\nERROR!")
                print(f"Source: {source}\nData: {res.content}")
                url = ""
        else: 
            return None
    # Loop through products to get individual product data
    for product in tqdm(data, ascii=True, desc="Download Raw Data"):
        res = requests.get(f"{src_url}/{product['productId']}", headers=product_headers)

        # Write raw file to folder structure
        if not os.path.exists(f"./data/raw/{source}/{current_date}/"):
            os.makedirs(f"./data/raw/{source}/{current_date}/")
        try:
            with open(f"./data/raw/{source}/{current_date}/{product['productId']}.json","w") as f:
                f.write(json.dumps(res.json()))
        except Exception as e:
            print("\nERROR!")
            print(f"Source: {source}\nProduct ID: {product['productId']}\nData: {res.content}")
            os.remove(f"./data/raw/{source}/{current_date}/{product['productId']}.json")
                

def load_model(source, model_entities):
    for entity_name, entity_df in tqdm(model_entities.items(),ascii=True,desc="Load Model Data"):
        output_file = f"./data/model/{source}/{entity_name}.csv"
        entity_df.to_csv(output_file, index=False, mode="a" if entity_name[:2] == "s_" else "w", header=not os.path.exists(output_file) or entity_name[:2] == "h_")

def load_conformed(entity, conformed_entity_df):
    for entity in tqdm([entity], ascii=True, desc="Load Conformed Data"):
        conformed_entity_df.to_csv(f"./data/conformed/{entity}.csv", index=False)