# Helper function to build a more structured format for the Print Group Genomics data

import pandas as pd
import numpy as np
import json
from pathlib import Path
from Core.helpers import RAiDFactory


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
            json.dump(json_dict, out_file, indent=4)
        return True
    except Exception as err:
        raise


sample_sheet = Path(
    '/home/chris/Projects/PrintGroupGenomics/sampleSheet_2020_08_31.xlsx')
duplicate_sheet = Path(
    '/home/chris/Projects/PrintGroupGenomics/Dups.xlsx')
project_json = Path('/home/chris/Projects/PrintGroupGenomics/project.json')
experiment_json = Path(
    '/home/chris/Projects/PrintGroupGenomics/experiment.json')
experiment_raid_json = Path(
    '/home/chris/Projects/PrintGroupGenomics/experiment_raid.json')
dataset_raid_json = Path(
    '/home/chris/Projects/PrintGroupGenomics/dataset_raid.json')
int1_sheet = Path('/home/chris/Projects/PrintGroupGenomics/Testing1.xlsx')
int2_sheet = Path('/home/chris/Projects/PrintGroupGenomics/Testing2.xlsx')
test_sheet = Path('/home/chris/Projects/PrintGroupGenomics/Testing.xlsx')
prosper_sheet = Path('/home/chris/Projects/PrintGroupGenomics/PROSPER.xlsx')
#project_sheet = Path('/home/chris/Projects/PrintGroupGenomics/Project.xlsx')
exp_sheet = Path('/home/chris/Projects/PrintGroupGenomics/Experiment.xlsx')
dataset_sheet = Path('/home/chris/Projects/PrintGroupGenomics/Dataset2.xlsx')
datafile_sheet = Path('/home/chris/Projects/PrintGroupGenomics/Datafile2.xlsx')
raw_data = pd.read_excel(sample_sheet, nrows=1086)
dups = pd.read_excel(duplicate_sheet)
prosper = pd.read_excel(prosper_sheet)
prosper.columns = ['Experiment_name',
                   'Old desc',
                   'New_desc',
                   'Prosper_desc']
global_config_file_path = Path(
    '/home/chris/Projects/MyTardisTestData/mech_global.env')

#raid_factory = RAiDFactory(global_config_file_path)

matches = {
    'Gastroblastoma': "10378.1/1634862",
    'siNET': "10378.1/1634856",
    'A0006': "10378.1/1634860",
    'Gastric': "10378.1/1634823",
    'NET-Plasma': "10378.1/1634872",
    'Phaeo-para': "10378.1/1634837",
    'pNET': "10378.1/1635063",
    'VIPoma': "10378.1/1634864",
    'CELL_LINE': "10378.1/1634847",
    'Merkel': "10378.1/1634819",
    'PROSPER': "10378.1/1635085",
    'Glio': "10378.1/1634845",
    'RA': "10378.1/1634859",
    'Pancreatoblastoma': "10378.1/1634860"
}

reserved = ['10378.1/1648899',
            '10378.1/1634851',
            '10378.1/1634879',
            '10378.1/1635081',
            '10378.1/1634873',
            '10378.1/1635078',
            '10378.1/1635084',
            '10378.1/1635083',
            '10378.1/1635096',
            '10378.1/1634876',
            '10378.1/1634877',
            '10378.1/1634867',
            '10378.1/1634850',
            '10378.1/1634882',
            '10378.1/1634863',
            '10378.1/1634880',
            '10378.1/1634848',
            '10378.1/1634855',
            '10378.1/1634874',
            '10378.1/1634830',
            '10378.1/1634826',
            '10378.1/1634811',
            '10378.1/1634822',
            '10378.1/1634839',
            '10378.1/1621957',
            '10378.1/1634849',
            '10378.1/1634828',
            '10378.1/1635045',
            '10378.1/1635071',
            '10378.1/1634841',
            '10378.1/1635048',
            '10378.1/1634857',
            '10378.1/1634823',
            '10378.1/1634862',
            '10378.1/1635085',
            '10378.1/1634819',
            '10378.1/1634860',
            '10378.1/1634837',
            '10378.1/1634872',
            '10378.1/1635063',
            '10378.1/1634859',
            '10378.1/1634856',
            '10378.1/1634864',
            '10378.1/1634890',
            '10378.1/1634847',
            '10378.1/1634844',
            '10378.1/1634845'
            ]

projects = readJSON(project_json)
raw_data['Project_Raid'] = raw_data['Project'].map(matches)

raw_data['Experiment_name'] = np.where(raw_data['Barcode'].isna(), raw_data[[
                                       'Patient ID', 'Sample Code']].apply(lambda x: ''.join(x.map(str)), axis=1), raw_data['Barcode'].astype(str))

'''response = raid_factory.get_all_raids()
resp_dict = json.loads(response.text)
raids = resp_dict['items']'''

experiments = readJSON(experiment_raid_json)
raw_data['Experiment_Raid'] = raw_data['Experiment_name'].map(experiments)
dups_dict = dups.set_index('Experiment_name')[
    'Description of Tumour'].to_dict()
keys_values = dups_dict.items()
dups_dict = {str(key): str(value) for key, value in keys_values}
prosper_dict = prosper.set_index('Experiment_name')['New_desc'].to_dict()
raw_data['Experiment_desc'] = raw_data['Experiment_name'].map(dups_dict)
raw_data.to_excel(int1_sheet)
raw_data['Experiment_desc'] = raw_data['Experiment_name'].map(
    prosper_dict).fillna(raw_data['Experiment_desc'])
raw_data.to_excel(int2_sheet)
prosper_dict = prosper.set_index('Experiment_name')['Prosper_desc'].to_dict()
raw_data['Experiment_desc'] = np.where(raw_data['Experiment_desc'].isna(
), raw_data['Description of Tumour'], raw_data['Experiment_desc'])
raw_data['Experiment_Tissue_Type'] = raw_data['Experiment_name'].map(
    prosper_dict)
raw_data['Experiment_Tissue_Type'] = np.where(raw_data['Experiment_Tissue_Type'].isna(
), raw_data['Tissue Type'], raw_data['Experiment_Tissue_Type'])
RA_fixup = {'RA': 'Lung neuroendocrine primary tumour and associated metastases'}
raw_data['Experiment_Tissue_Type'] = raw_data['Experiment_Tissue_Type'].map(
    RA_fixup).fillna(raw_data['Experiment_Tissue_Type'])
raw_data['Experiment_Tissue_Type']
raw_data['Dataset_name'] = raw_data[['Experiment_name', 'Type']].apply(
    lambda x: '-'.join(x.map(str)), axis=1)
exps = raw_data[['Project_Raid',
                 'Experiment_name',
                 'Experiment_desc',
                 'Experiment_Raid',
                 'Patient ID',
                 'Sample Code',
                 'Experiment_Tissue_Type',
                 'Tumour/Normal',
                 'Long Description']].drop_duplicates(subset='Experiment_name')
exps.to_excel(exp_sheet)
exps.columns = ['project_id',
                'title',
                'description',
                'raid',
                'exp_patient_id',
                'exp_sample_code',
                'exp_tissue_type',
                'exp_tissue_designation',
                'exp_long_description']

expt_dict = {"schema": "https://mytardis.nectar.auckland.ac.nz/FHMS/PrintGroupGenomics/v1/Experiment",
             "admin_groups": ["PGG_admin"],
             "admins": ["cpri004"],
             "url": "http://www.google.co.nz"}

expt_dict['experiments'] = exps.T.apply(
    lambda x: x.dropna().to_dict()).tolist()
print(len(expt_dict['experiments']))
writeJSON(expt_dict, experiment_json)

'''datasets = {}
for dataset in list(set(raw_data['Dataset_name'])):
    response = raid_factory.mint_raid(dataset,
                                      'PGG-Assay',
                                      'http://google.co.nz')
    resp_dict = json.loads(response.text)
    datasets[dataset] = resp_dict['handle']
    writeJSON(datasets, dataset_raid_json)
'''
datasets = readJSON(dataset_raid_json)
raw_data['Dataset_Raid'] = raw_data['Dataset_name'].map(datasets)

datset = raw_data[['Experiment_Raid',
                   'Dataset_name',
                   'Dataset_Raid',
                   'Supplier',
                   'Type',
                   'Macrogen Quote #',
                   'Macrogen Work Order',
                   'Purchase Order #',
                   'Date Sample Sent']]
datset.to_excel(dataset_sheet)

datfile = raw_data[['Experiment_name',
                    'Dataset_Raid',
                    'Run number',
                    'Sequencing Notes',
                    'resmed Location',
                    'QC']]

datfile.to_excel(datafile_sheet)

raw_data.to_excel(test_sheet)
