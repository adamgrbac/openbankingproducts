import requests
import json
from datetime import date

# Get current date for file structure
current_date = date.today().strftime('%Y%m%d')

# Open Banking API required headers
# x-v: Requesting Open Banking API Version
# x-v-min: Minimum Acceptable Open Banking API Versiom
product_headers = {"x-v":"3", "x-v-min":"2"}

# ANZ Products API
url = "https://api.anz/cds-au/v1/banking/products"

# Initialise Empty Array
data = []

# Get products
# N.B. products data is pageinated so need to iterate through until at the end
while url != "":
    res = requests.get(url, headers = product_headers)
    if res.status_code == 200:
        data.extend(res.json()["data"]["products"])
        url = res.json()["links"].get("next","")

# Loop through products to get individual product data
for product in data:
    print(f"Getting details for {product['name']}...")
    res = requests.get(f"https://api.anz/cds-au/v1/banking/products/{product['productId']}", headers=product_headers)

    # Write raw file to folder structure
    with open(f"./ANZ/{current_date}/{product['productId']}.json","w") as f:
        f.write(json.dumps(res.json()))
