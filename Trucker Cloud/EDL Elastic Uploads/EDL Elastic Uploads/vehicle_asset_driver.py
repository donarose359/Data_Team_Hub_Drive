# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 15:21:10 2024

@author: shrihari
"""

import requests
import pandas as pd
from authentication import get_authtoken
from flatten_data import flatten_dict

veh_df = pd.read_excel('Input/Vehicles.xlsx')
authToken = get_authtoken()
headers = {
    "accept": "*/*",
    "Authorization": authToken
    }
veh_asset_dri_li = []
for v in range(len(veh_df)):
    veh_asset_dri_url = "https://api.truckercloud.com/api/v4/vehicles/"+str(veh_df['assetId'].iloc[v])+"/driver"
    print(veh_asset_dri_url)
    response = requests.get(veh_asset_dri_url, headers=headers).json()
    veh_asset_dri = response
    temp_li = []
    temp_li.append(veh_asset_dri)
    for item in temp_li:
        flat_data = flatten_dict(item)
        veh_asset_dri_li.append(flat_data)
        
# Create DataFrame from flattened data
df = pd.DataFrame(veh_asset_dri_li)
df.to_excel('Input/veh_asset_driver.xlsx',index=False)