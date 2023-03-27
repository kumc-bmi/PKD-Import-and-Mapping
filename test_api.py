#!/usr/bin/env python

import requests
import pandas as pd
from io import StringIO

data = {
    'token': '9F01E61ABBDAED93744274003B4E7360',
    'content': 'metadata',
    'format': 'csv',
    'returnFormat': 'json'
}
r = requests.post('https://redcap.kumc.edu/api/', data=data)

temp_df_uab = str(r.content,'utf-8')
data_uab = StringIO(temp_df_uab)
uab_int_df = pd.read_csv(data_uab, header=0, encoding='utf-8')

uab_int_df.to_csv("./Test_csvs/kumc_data_dict.csv", encoding='utf-8')
