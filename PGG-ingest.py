from pathlib import Path
from PrintGroup import PGPIngestFactory
#from Core import MyTardisRESTFactory
#import json
#from Core.helpers import writeJSON

local_config_file_path = Path(
    '/home/chris/Projects/PrintGroupGenomics/local.env')
local_config_file_path = Path(
    '/home/chris/Projects/MyTardisTestData/mech_local.env')
global_config_file_path = Path(
    '/home/chris/Projects/PrintGroupGenomics/global.env')
project_json_path = Path(
    '/home/chris/Projects/PrintGroupGenomics/project.json')
experiment_json_path = Path(
    '/home/chris/Projects/PrintGroupGenomics/experiment.json')
existing_json_path = Path(
    '/home/chris/Projects/PrintGroupGenomics/experiment_exist.json')
dataset_json_path = Path(
    '/home/chris/Projects/PrintGroupGenomics/dataset.json')

factory = PGPIngestFactory(
    local_config_file_path,
    global_config_file_path,
    project_json_path,
    experiment_json_path,
    dataset_json_path)

'''project_list = factory.process_project_input()
for project in project_list:
    uri, project_id = factory.forge_project(project)
    response = factory.update_project(project_id,
                                      project)'''
experiments = factory.process_experiment_input()
for experiment in experiments:
    #experiment = experiments[0]
    #print(experiment)
    uri, experiment_id = factory.reforge_experiment(experiment)
    response = factory.update_experiment(experiment_id,
                                         experiment)

datasets = factory.process_dataset_input()
for dataset in datasets:
    uri, dataset_id = factory.reforge_dataset(dataset)
    response = factory.update_dataset(dataset_id,
                                      dataset)

