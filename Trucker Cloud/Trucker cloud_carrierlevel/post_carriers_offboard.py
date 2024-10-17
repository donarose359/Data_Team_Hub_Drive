# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 11:50:56 2024

@author: donar
"""

import requests
import pandas as pd

def post_carriers_offboard():
    url = "https://api.truckercloud.com/api/v4/carriers/offboard?carrierName=AMERICAN%20TRANSPORTATION&codeType=DOT&codeValue=1151228"
    
    headers = {
        "accept": "application/json",
        "Authorization": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJoZG9sIiwiaXNzIjoiaHR0cDovL3d3dy50cnVja2VyY2xvdWQuY29tLyIsImlhdCI6MTcyNjcyNDU4NCwiZXhwIjoxNzI2ODEwOTg0fQ.dMiAzm8IKXDJdQ2CGKQBfwNvScR_68T_AcFFumWprjk"
    }
    
    response = requests.post(url, headers=headers)
    
    print(response.text)
    df_post_carriers_offboard=pd.read_json(response.text)
    return df_post_carriers_offboard
df_post_carriers_offboard=post_carriers_offboard()
#df_post_carriers_offboard.to_excel(r"D:\hdo\Trucker cloud_carrierlevel\post_carriers_offboard.xlsx", index=False)