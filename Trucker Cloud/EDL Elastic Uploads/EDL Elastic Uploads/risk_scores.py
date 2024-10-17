# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 16:39:37 2024

@author: shrihari
"""

import pandas as pd
from authentication import get_authtoken
import requests
from flatten_data import flatten_dict


authToken = get_authtoken()

url = "https://api.truckercloud.com/api/v5/risk-scores"

headers = {
    "accept": "*/*",
    "Authorization": authToken
}


response = requests.get(url, headers=headers).json()
risk_scores_li = response
flat_risk_scores_li = []
rsik_scores_only_li = []

risk_scores_only = risk_scores_li[0].pop('riskScores')
for item in risk_scores_li:
    flat_data = flatten_dict(item)
    flat_risk_scores_li.append(flat_data)
    
# Create DataFrame from flattened data
# df = pd.DataFrame(flat_risk_scores_li)

for item in risk_scores_only:
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


df1.to_excel('Input/risk_scores_carrier_level.xlsx',index=False)