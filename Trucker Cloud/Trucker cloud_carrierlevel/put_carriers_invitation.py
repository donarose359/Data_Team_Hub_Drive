# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 11:29:59 2024

@author: donar
"""

import requests
import pandas as pd

def put_carriers_invitation():

    url = "https://api.truckercloud.com/api/v4/carriers/invitation?carrierName=AMERICAN%20TRANSPORTATION&codeType=DOT&codeValue=1151228&email=sam%2B135409335%40r1vs.com"
    
    headers = {
        "accept": "*/*",
        "Authorization": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJoZG9sIiwiaXNzIjoiaHR0cDovL3d3dy50cnVja2VyY2xvdWQuY29tLyIsImlhdCI6MTcyNjcyNDU4NCwiZXhwIjoxNzI2ODEwOTg0fQ.dMiAzm8IKXDJdQ2CGKQBfwNvScR_68T_AcFFumWprjk"
    }
    
    response = requests.put(url, headers=headers)
    
    print(response.text)
    print(response.text)
    df_put_carriers_invitation=pd.read_json(response.text)
    return df_put_carriers_invitation
df_put_carriers_invitation=put_carriers_invitation()
#df_put_carriers_invitation.to_excel(r"D:\hdo\Trucker cloud_carrierlevel\put_carriers_invitation.xlsx", index=False)

