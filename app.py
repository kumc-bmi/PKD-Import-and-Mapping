"""
Authors:
- Mary Penne Mays
- Siddharth Satyakam
- Sravani Chandakaq
- Lav Patel
"""
import configparser
import logging
import pandas as pd
import os
import requests
import csv
import pysftp
from sys import argv
from os import path as os_path
from builtins import open as openf
from itertools import groupby

from .extract import redcap_export_api
from .transform import mapped_csvs
from .load import redcap_import_api

# export site(s) csvs from redcap through API
redcap_export_api()
# map sites redcap projects and export to csvs
mapped_csvs()
# import all converted site(s) csvs into redcap through API
redcap_import_api()
