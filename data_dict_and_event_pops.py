#!/usr/bin/env python

import requests
import pandas as pd
from io import StringIO
import os
from dotenv import load_dotenv

load_dotenv()

# Get data dictionary for the final import project:
dataconfig_datadict = dict()
dataconfig_datadict["token"] = os.getenv('import_data_dict_token')

for key, value in os.environ.items():
    if key.startswith('import_data_dict_'):
        key_entry = key.split('import_data_dict_')[1]
        dataconfig_datadict[key_entry] = value
        
print(dataconfig_datadict)

r = requests.post('https://redcap.kumc.edu/api/', data=dataconfig_datadict)

data_dict_res = str(r.content,'utf-8')
data_dict_multisite = StringIO(data_dict_res)
PKDMultisiteRegistry_DataDictionary = pd.read_csv(data_dict_multisite, header=0, encoding='utf-8')

PKDMultisiteRegistry_DataDictionary.to_csv("./Mapping_csvs/PKDMultisiteRegistry_DataDictionary.csv", encoding='utf-8', index=False)

# Get events for the final import project:
dataconfig_events = dict()
dataconfig_events["token"] = os.getenv('import_data_dict_token')

for key, value in os.environ.items():
    if key.startswith('import_final_events_'):
        key_entry = key.split('import_final_events_')[1]
        dataconfig_events[key_entry] = value

print(dataconfig_events)
req = requests.post('https://redcap.kumc.edu/api/', data=dataconfig_events)

events_res = str(req.content,'utf-8')
events_multisite = StringIO(events_res)
PKDMultisiteRegistry_events = pd.read_csv(events_multisite, header=0, encoding='utf-8')

PKDMultisiteRegistry_events.to_csv("./Mapping_csvs/PKDMultisiteRegistry_events.csv", encoding='utf-8', index=False)
