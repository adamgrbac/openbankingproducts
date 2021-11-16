from datetime import date, timedelta
import pandas as pd


def maybe_print(name, value):
    if pd.isna(value):
        pass
    else:
        print(f"\t\t\t  {name}: {value}")


# It's just a jump to the left
TIMEWARP = 0
current_date = date.today() - timedelta(days=TIMEWARP)

h_product = pd.read_csv("./data/conformed/h_product.csv")
s_productdetails = pd.read_csv("./data/conformed/s_productdetails.csv")
s_lendingrates = pd.read_csv("./data/conformed/s_lendingrates.csv")

s_lendingrates_today = s_lendingrates[s_lendingrates.eftv_date == current_date.strftime('%Y-%m-%d')]
s_lendingrates_yday = s_lendingrates[s_lendingrates.eftv_date == (current_date - timedelta(days=1)).strftime('%Y-%m-%d')]

delta_frame = s_lendingrates_today.merge(s_lendingrates_yday,
                                         on=["source",
                                             "productId",
                                             "lendingRateType",
                                             "loanPurpose",
                                             "repaymentType",
                                             "interestPaymentDue",
                                             "applicationFrequency",
                                             "additionalInfo",
                                             "additionalValue",
                                             "min_lvr",
                                             "max_lvr",
                                             "min_term",
                                             "max_term",
                                             "min_val",
                                             "max_val"],
                                         how="outer",
                                         indicator=True)

delta_frame["rate_x"] = delta_frame["rate_x"].round(6)
delta_frame["rate_y"] = delta_frame["rate_y"].round(6)
print(f"Rate changes for {current_date.strftime('%Y-%m-%d')}")

# Inserts
print("Checking for New Rates...")
inserts = delta_frame[delta_frame._merge == "left_only"]
print(f"\t> {len(inserts)} new rates found!")
sources = inserts.source.unique()
with pd.ExcelWriter(f"outputs/{current_date.strftime('%Y%m%d')}_rate_changes.xlsx", mode="w", engine="openpyxl") as writer:
    join_df = inserts.merge(s_productdetails[s_productdetails.eftv_date == current_date.strftime('%Y-%m-%d')], on=["source", "productId"], how="left")
    input_cols = ["source",
                  "name",
                  "lendingRateType",
                  "loanPurpose",
                  "repaymentType",
                  "interestPaymentDue",
                  "applicationFrequency",
                  "additionalInfo",
                  "additionalValue",
                  "min_lvr",
                  "max_lvr",
                  "min_term",
                  "max_term",
                  "min_val",
                  "max_val",
                  "rate_x"]
    filtered_join_df = join_df[input_cols].sort_values(by=["source", "name"])
    filtered_join_df.columns = ["Institution",
                                "Product Name",
                                "Rate Type",
                                "Purpose",
                                "Repayment Type",
                                "Interest Due",
                                "Interest Frequency",
                                "Additional Info",
                                "Additional Value",
                                "Min LVR",
                                "Max LVR",
                                "Min Term",
                                "Max Term",
                                "Min Value",
                                "Max Value",
                                "New Rate"]
    filtered_join_df.to_excel(writer, sheet_name="New Rates", index=False)

# Updates
print("Checking for Updated Rates...")
updates = delta_frame[(delta_frame._merge == "both") & (delta_frame.rate_x.mask(pd.isnull, 0) != delta_frame.rate_y.mask(pd.isnull, 0))]
sources = updates.source.unique()
print(f"\t> {len(updates)} updated rates found!")
with pd.ExcelWriter(f"outputs/{current_date.strftime('%Y%m%d')}_rate_changes.xlsx", mode="a", engine="openpyxl") as writer:
    join_df = updates.merge(s_productdetails[s_productdetails.eftv_date == current_date.strftime('%Y-%m-%d')], on=["source", "productId"], how="left")
    input_cols = ["source",
                  "name",
                  "lendingRateType",
                  "loanPurpose",
                  "repaymentType",
                  "interestPaymentDue",
                  "applicationFrequency",
                  "additionalInfo",
                  "additionalValue",
                  "min_lvr",
                  "max_lvr",
                  "min_term",
                  "max_term",
                  "min_val",
                  "max_val",
                  "rate_y",
                  "rate_x"]
    filtered_join_df = join_df[input_cols].sort_values(by=["source", "name"])
    filtered_join_df.columns = ["Institution",
                                "Product Name",
                                "Rate Type",
                                "Purpose",
                                "Repayment Type",
                                "Interest Due",
                                "Interest Frequency",
                                "Additional Info",
                                "Additional Value",
                                "Min LVR",
                                "Max LVR",
                                "Min Term",
                                "Max Term",
                                "Min Value",
                                "Max Value",
                                "Old Rate",
                                "New Rate"]
    filtered_join_df.to_excel(writer, sheet_name="Updated Rates", index=False)

# Deletes
print("Checking for Deleted Rates...")
deletes = delta_frame[delta_frame._merge == "right_only"]
sources = deletes.source.unique()
print(f"\t> {len(deletes)} deleted rates found!")
with pd.ExcelWriter(f"outputs/{current_date.strftime('%Y%m%d')}_rate_changes.xlsx", mode="a", engine="openpyxl") as writer:
    join_df = deletes.merge(s_productdetails[s_productdetails.eftv_date == (current_date - timedelta(days=1)).strftime('%Y-%m-%d')], on=["source", "productId"], how="left")
    input_cols = ["source",
                  "name",
                  "lendingRateType",
                  "loanPurpose",
                  "repaymentType",
                  "interestPaymentDue",
                  "applicationFrequency",
                  "additionalInfo",
                  "additionalValue",
                  "min_lvr",
                  "max_lvr",
                  "min_term",
                  "max_term",
                  "min_val",
                  "max_val",
                  "rate_y"]
    filtered_join_df = join_df[input_cols].sort_values(by=["source", "name"])
    filtered_join_df.columns = ["Institution",
                                "Product Name",
                                "Rate Type",
                                "Purpose",
                                "Repayment Type",
                                "Interest Due",
                                "Interest Frequency",
                                "Additional Info",
                                "Additional Value",
                                "Min LVR",
                                "Max LVR",
                                "Min Term",
                                "Max Term",
                                "Min Value",
                                "Max Value",
                                "Old Rate"]
    filtered_join_df.to_excel(writer, sheet_name="Deleted Rates", index=False)
