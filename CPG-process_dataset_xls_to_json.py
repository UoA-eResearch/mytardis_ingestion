# Helper function to build a more structured format for the Print Group Genomics data

import pandas as pd
import numpy as np
import json
from pathlib import Path
import datetime


def readJSON(json_file):
    try:
        with open(json_file, 'r') as in_file:
            json_dict = json.load(in_file)
    except Exception as err:
        raise
    return json_dict


def writeJSON(json_dict, json_file):
    try:
        with open(json_file, 'w') as out_file:
            json.dump(json_dict, out_file, indent=4, default=date_handler)
        return True
    except Exception as err:
        raise


def date_handler(obj): return (
    obj.isoformat()
    if isinstance(obj, datetime.date)
    or isinstance(obj, datetime.datetime)
    else None
)


dataset_sheet = Path(
    '/home/chris/Projects/PrintGroupGenomics/Dataset.xlsx')
dataset_json = Path(
    '/home/chris/Projects/PrintGroupGenomics/dataset.json')

dataset_data = pd.read_excel(dataset_sheet)
dataset_data.columns = ['experiments',
                        'description',
                        'dataset_id',
                        'dataset_supplier',
                        'dataset_type',
                        'dataset_quote',
                        'dataset_work_order',
                        'dataset_purchase_order',
                        'dataset_sent_date']

instruments = {'AgResearch_Otago': 'AgResearch',
               'Macrogen': 'Macrogen',
               'pNET paper': 'Historic',
               'Auckland_Genomics': 'Auckland_Genomics',
               'AGRF': 'AGRF'}

suppliers = {'AgResearch_Otago': 'AgResearch Otago',
             'Auckland_Genomics': 'Auckland Genomics',
             'AGRF': 'Australian Genome Research Facility'}

dataset_data['instrument_id'] = dataset_data['dataset_supplier'].map(
    instruments)
dataset_data['dataset_supplier'] = dataset_data['dataset_supplier'].map(
    suppliers).fillna(dataset_data['dataset_supplier'])
dataset_dict = {"schema": "https://mytardis.nectar.auckland.ac.nz/FMHS/PrintGroupGenomics/WES-WGS-biopipeline-WESt/v1/Dataset",
                "admin_groups": ["PGG_admin"],
                "admins": ["cpri004"],
                "url": "http://www.google.co.nz"}
dataset_dict['datasets'] = dataset_data.T.apply(
    lambda x: x.dropna().to_dict()).tolist()
writeJSON(dataset_dict, dataset_json)
print(dataset_data)
