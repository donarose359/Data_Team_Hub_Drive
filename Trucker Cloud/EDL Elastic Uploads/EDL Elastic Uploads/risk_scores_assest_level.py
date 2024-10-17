# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 17:24:22 2024

@author: shrihari
"""

import pandas as pd
from authentication import get_authtoken
import requests
from flatten_data import flatten_dict

df_veh =pd.read_excel('Input/Vehicles.xlsx')

authToken = get_authtoken()

headers = {
    "accept": "*/*",
    "Authorization": authToken
}

flat_risk_scores_li = []
for a in range(len(df_veh)): #asset loop
    asset_risk_score_url = "https://api.truckercloud.com/api/v5/risk-scores?assetId="+str(df_veh['assetEldId'][a])+"&assetIdType=assetEldId&carrierCode="+str(df_veh['carrierCodes_carrierCode'][a])+"&codeType="+str(df_veh['carrierCodes_codeType'][a])+"&eldVendor="+str(df_veh['eldVendors_eldVendor'][a])+"&showAssetDetails=true"
    print(asset_risk_score_url)
    response = requests.get(asset_risk_score_url, headers=headers).json()
    risk_scores_li = response
    risk_scores_carrier = risk_scores_li[0].pop('riskScores')
    risk_scores_asset = risk_scores_li[0].pop('riskScoresByAsset')
    
    for item in risk_scores_li:
        flat_data = flatten_dict(item)
        flat_risk_scores_li.append(flat_data)

    for item in risk_scores_asset:
        flat_data = flatten_dict(item)
        flat_risk_scores_li.append(flat_data)
    
# Create DataFrame from flattened data
df1 = pd.DataFrame(flat_risk_scores_li)

# Identify columns that need to be forward filled
columns_to_fill = df1.columns[:4]  # Assuming the first four columns need to be filled down

# Forward fill the missing values in the identified columns
df1[columns_to_fill] = df1[columns_to_fill].ffill()
df1 = df1.dropna(subset=['score'])
# Reset index after dropping rows
df1 = df1.reset_index(drop=True)

    

df1.to_excel('Input/risk_scores_asset_level.xlsx',index=False)