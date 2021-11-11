import ETL
from datetime import date, timedelta
import yaml

current_date = date.today()# - timedelta(days=1)

with open("./config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

# ETL from RAW -> MODEL   
print("""########################
### Raw -> Model ETL ###
########################""")
 
for source, url in config["raw_sources"].items():
    
    print(f"Running Raw -> Model ETL for {source}...")
    
    # Download raw files
    ETL.load_raw(source, url, current_date)
    
    # Extract raw files
    products = ETL.extract_raw(f"./data/raw/{source}/{current_date.strftime('%Y%m%d')}")
    
    # Transform raw data into model format
    model_entities = ETL.transform_raw(source, products, current_date)
    
    # Load model data into files
    ETL.load_model(source,model_entities)

# ETL from MODEL -> CONFORMED
print("""##############################
### Model -> Conformed ETL ###
##############################""")

for entity in config["model_entities"]:

    print(f"Running Model -> Conformed ETL for {entity}...")
    
    model = ETL.extract_model(entity, config["raw_sources"].keys())
    
    ETL.load_conformed(entity, model)