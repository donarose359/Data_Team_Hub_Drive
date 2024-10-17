# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 14:31:53 2024

@author: shrihari
"""

import pandas as pd
from authentication import get_authtoken
import requests
from date_range import get_date_ranges
from datetime import date
from flatten_data import flatten_dict

df=pd.read_excel('Input/carriers.xlsx')
print("File Found")
ddf = df[df['codeType'] == 'DOT']
authToken = get_authtoken()

headers = {
    "accept": "*/*",
    "Authorization": authToken
    }


start_date = date(2024, 1, 1)
today = date.today()
date_ranges = get_date_ranges(start_date, today)

flat_asset_travel_li = []
for c in range(len(ddf)):
    for d in range(len(date_ranges)):
        if date_ranges[d][1] > date_ranges[d][0]:
            url = "https://api.truckercloud.com/api/v4/enriched-data/gps-miles?carrierCodeType=DOT&carrierCodeValue="+str(ddf['carrierCode'].iloc[c])+"&eldVendor="+str(ddf['eldVendor'].iloc[c])+"&endDateTime="+str(date_ranges[d][1])+"T00%3A00%3A00Z&startDateTime="+str(date_ranges[d][0])+"T00%3A00%3A00Z"
            response = requests.get(url, headers=headers).json()
            asset_travel = response['gpsMilesByAsset']
            for item in asset_travel:
                item.update({"start_date":str(date_ranges[d][0])})
                item.update({"end_date":str(date_ranges[d][1])})
                flat_data = flatten_dict(item)
            flat_asset_travel_li.append(flat_data)
            
df = pd.DataFrame(flat_asset_travel_li)
df.to_excel('Input/asset_travel.xlsx',index= False)
