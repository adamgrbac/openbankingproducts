import ETL
import pandas as pd
from datetime import date, timedelta
import yaml

def maybe_print(name, value):
    if pd.isna(value):
        return None
    print(f"\t\t\t  {name}: {value}")
    
timewarp = 4
current_date = date.today() - timedelta(days=timewarp)
 
h_product = pd.read_csv("./data/conformed/h_product.csv")
s_productdetails = pd.read_csv("./data/conformed/s_productdetails.csv")
s_lendingrates = pd.read_csv("./data/conformed/s_lendingrates.csv")

s_lendingrates_today = s_lendingrates[s_lendingrates.eftv_date == (date.today()- timedelta(days=timewarp)).strftime('%Y-%m-%d')]
s_lendingrates_yday = s_lendingrates[s_lendingrates.eftv_date == (date.today() - timedelta(days=timewarp+1)).strftime('%Y-%m-%d')]

delta_frame = s_lendingrates_today.merge(s_lendingrates_yday, on=["source","productId","lendingRateType","loanPurpose","repaymentType","interestPaymentDue","applicationFrequency","additionalInfo","additionalValue","min_lvr","max_lvr","min_term","max_term", "min_val", "max_val"], how="outer", indicator=True)

print(f"Rate changes for {current_date.strftime('%Y-%m-%d')}")

# Inserts
print("New Rates:")
inserts = delta_frame[delta_frame._merge=="left_only"]
sources = inserts.source.unique()
products = inserts.productId.unique()
if len(inserts) == 0:
    print("\t> No New rates!")
for source in sources:
    print(f"\t> Bank: {source}")
    for product in products:
        print(f"\t\t> Product: {s_productdetails[(s_productdetails.eftv_date == current_date.strftime('%Y-%m-%d')) & (s_productdetails.productId == product)].name.values}")
        for idx, row in inserts[(inserts.source==source) & (inserts.productId==product)].iterrows():
            print(f"\n\t\t\t> Rate Type: {row['lendingRateType']};")
            maybe_print("Purpose", row['loanPurpose'])
            maybe_print("Repayment Type", row['repaymentType'])
            maybe_print("Interest Due", row['interestPaymentDue'])
            maybe_print("Interest Frequency", row['applicationFrequency'])
            maybe_print("Additional Info", row['additionalInfo'])
            maybe_print("Additional Value", row['additionalValue'])
            maybe_print("Min LVR", row['min_lvr'])
            maybe_print("Max LVR", row['max_lvr'])
            maybe_print("Min Term", row['min_term'])
            maybe_print("Max Term", row['max_term'])
            maybe_print("Min Val", row['min_val'])
            maybe_print("Max Val", row['max_val'])
            print(f"\t\t\t  New Rate: {row['rate_x']}")

# Updates
print("Updated Rates:")
updates = delta_frame[(delta_frame._merge=="both") & (delta_frame.rate_x.mask(pd.isnull,0) != delta_frame.rate_y.mask(pd.isnull,0))]
sources = updates.source.unique()
products = updates.productId.unique()
if len(updates) == 0:
    print("\t> No Updated rates!")
for source in sources:
    print(f"\t> Bank: {source}")
    for product in products:
        print(f"\t\t> Product: {s_productdetails[(s_productdetails.eftv_date == current_date.strftime('%Y-%m-%d')) & (s_productdetails.productId == product)].name.values}")
        for idx, row in updates[(updates.source==source) & (updates.productId==product)].iterrows():
            print(f"\n\t\t\t> Rate Type: {row['lendingRateType']};")
            maybe_print("Purpose", row['loanPurpose'])
            maybe_print("Repayment Type", row['repaymentType'])
            maybe_print("Interest Due", row['interestPaymentDue'])
            maybe_print("Interest Frequency", row['applicationFrequency'])
            maybe_print("Additional Info", row['additionalInfo'])
            maybe_print("Additional Value", row['additionalValue'])
            maybe_print("Min LVR", row['min_lvr'])
            maybe_print("Max LVR", row['max_lvr'])
            maybe_print("Min Term", row['min_term'])
            maybe_print("Max Term", row['max_term'])
            maybe_print("Min Val", row['min_val'])
            maybe_print("Max Val", row['max_val'])
            print(f"\t\t\t  Old Rate: {row['rate_y']}")
            print(f"\t\t\t  New Rate: {row['rate_x']}")

# Deletes
print("Deleted Rates")
deletes = delta_frame[delta_frame._merge=="right_only"]
sources = deletes.source.unique()
products = deletes.productId.unique()
if len(deletes) == 0:
    print("\t> No Deleted rates!")
for source in sources:
    print(f"\t> Bank: {source}")
    for product in products:
        print(f"\t\t> Product: {s_productdetails[(s_productdetails.eftv_date == (current_date  - timedelta(days=1)).strftime('%Y-%m-%d')) & (s_productdetails.productId == product)].name.values}")
        for idx, row in deletes[(deletes.source==source) & (deletes.productId==product)].iterrows():
            print(f"\n\t\t\t> Rate Type: {row['lendingRateType']};")
            maybe_print("Purpose", row['loanPurpose'])
            maybe_print("Repayment Type", row['repaymentType'])
            maybe_print("Interest Due", row['interestPaymentDue'])
            maybe_print("Interest Frequency", row['applicationFrequency'])
            maybe_print("Additional Info", row['additionalInfo'])
            maybe_print("Additional Value", row['additionalValue'])
            maybe_print("Min LVR", row['min_lvr'])
            maybe_print("Max LVR", row['max_lvr'])
            maybe_print("Min Term", row['min_term'])
            maybe_print("Max Term", row['max_term'])
            maybe_print("Min Val", row['min_val'])
            maybe_print("Max Val", row['max_val'])
            print(f"\t\t\t  Old Rate: {row['rate_y']}")