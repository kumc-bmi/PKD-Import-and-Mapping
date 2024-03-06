"""
Authors:
- Mary Penne Mays
- Siddharth Satyakam
- Sravani Chandaka
- Lav Patel
"""
import os
import sys

# set directory path
dir_path = "/var/lib/jenkins/jobs/PKI MULTI-SITE REGISTRY/workspace/"

# set system path
sys.path.append(os.path.abspath(dir_path))

from extract import redcap_export_api
from transform import mapped_csvs
from load import redcap_import_api

# export site(s) csvs from redcap through API
redcap_export_api()
# map sites redcap projects and export to csvs
mapped_csvs()
# import all converted site(s) csvs into redcap through API
redcap_import_api()
