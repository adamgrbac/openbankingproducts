import pandas as pd
from tqdm import tqdm

def transform_raw(source, products, current_date):

    h_product = []
    s_productdetails = []
    s_fees = []
    s_eligibility = []
    s_features = []
    s_lendingrates = []
    s_depositrates = []
    s_constraints = []
    
    for product in tqdm(products, ascii=True, desc="Transform Raw -> Model"):
        # Construct Product Hub table
        h_product.append({"source": source,
                          "productId": product["data"]["productId"],
                          "lastUpdated": product["data"]["lastUpdated"]})
        # Construct Product Details Satellite Table
        s_productdetails.append({"source": source,
                                 "productId": product["data"]["productId"],
                                 "name": product["data"]["name"],
                                 "description": product["data"]["description"],
                                 "productCategory": product["data"]["productCategory"],
                                 "isTailored": product["data"]["isTailored"],
                                 "eftv_date": current_date})
        # Construct Product Fees Satellite Table
        for fee in product["data"].get("fees",[]):
            s_fees.append({"source": source,
                           "productId": product["data"]["productId"],
                           "name": fee["name"],
                           "feeType": fee["feeType"],
                           "currency": fee.get("currency",""),
                           "amount": fee.get("amount",""),
                           "transactionRate": fee.get("transactionRate",""),
                           "additionalValue": fee.get("additionalValue",""),
                           "additionalInfo": fee.get("additionalInfo",""),
                           "eftv_date": current_date})
        # Construct Product Eligibility Satellite Table
        for eligibility in product["data"].get("eligibility",[]):
            s_eligibility.append({"source": source,
                                  "productId": product["data"]["productId"],
                                  "eligibilityType": eligibility["eligibilityType"],
                                  "additionalValue": eligibility.get("additionalValue",""),
                                  "additionalInfo": eligibility.get("additionalInfo",""),
                                  "eftv_date": current_date})
        # Construct Product Features Satellite Table
        for feature in product["data"].get("features",[]):
            s_features.append({"source": source,
                               "productId": product["data"]["productId"],
                               "featureType": feature["featureType"],
                               "additionalValue": feature.get("additionalValue",""),
                               "additionalInfo": feature.get("additionalInfo",""),
                               "eftv_date": current_date})
        # Construct Product Lending Rates Satellite Table
        for lendingrate in product["data"].get("lendingRates",[]):
            s_lendingrates.append({"source": source,
                                   "productId": product["data"]["productId"],
                                   "lendingRateType": lendingrate["lendingRateType"],
                                   "loanPurpose": lendingrate.get("loanPurpose",""),
                                   "repaymentType": lendingrate.get("repaymentType",""),
                                   "rate": lendingrate.get("rate",""),
                                   "comparisonRate": lendingrate.get("comparisonRate",""),
                                   "interestPaymentDue": lendingrate.get("interestPaymentDue",""),
                                   "applicationFrequency": lendingrate.get("applicationFrequency",""),
                                   "calculationFrequency": lendingrate.get("calculationFrequency",""),
                                   "additionalInfo": lendingrate.get("additionalInfo",""),
                                   "additionalValue": lendingrate.get("additionalValue",""),
                                   "eftv_date": current_date})
        # Construct Product Deposit Rates Satellite Table
        for depositrate in product["data"].get("depositRates",[]):
            s_depositrates.append({"source": source,
                                   "productId": product["data"]["productId"],
                                   "depositRateType": depositrate.get("depositRateType",""),
                                   "rate": depositrate.get("rate",""),
                                   "calculationFrequency": depositrate.get("calculationFrequency",""),
                                   "additionalInfo": depositrate.get("additionalInfo",""),
                                   "additionalValue": depositrate.get("additionalValue",""),
                                   "eftv_date": current_date})
        for constraint in product["data"].get("constraints",[]):
            s_constraints.append({"source": source,
                                  "productId": product["data"]["productId"],
                                  "constraintType": constraint.get("constraintType",""),
                                  "additionalInfo": constraint.get("additionalInfo",""),
                                  "additionalValue": constraint.get("additionalValue",""),
                                  "eftv_date": current_date})
                                  
    return {"h_product": pd.DataFrame(h_product),
            "s_productdetails": pd.DataFrame(s_productdetails),
            "s_fees": pd.DataFrame(s_fees),
            "s_eligibility": pd.DataFrame(s_eligibility),
            "s_features": pd.DataFrame(s_features),
            "s_lendingrates": pd.DataFrame(s_lendingrates),
            "s_depositrates": pd.DataFrame(s_depositrates),
            "s_constraints": pd.DataFrame(s_constraints)}