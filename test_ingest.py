# test_ingest.py
#
# Ingestion test script for mechanical test data
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 07 Aug 2020
#

from mech_test import MechTestFactory
from pathlib import Path
from Core.filehandling.s3_filehandler import S3FileHandler

local_config_file_path = Path(
    '/home/chris/Projects/MyTardisTestData/mech_local.env')
raw_data_dir = Path('/home/chris/Projects/MyTardisTestData/mechtests/raw data')
cyclic_data_dir = Path(
    '/home/chris/Projects/MyTardisTestData/mechtests/processed_data/full_curves')
tensile_data_dir = Path(
    '/home/chris/Projects/MyTardisTestData/mechtests/processed_data/tension_tests')
plot_dir = Path('/home/chris/Projects/MyTardisTestData/mechtests/TestPlots')
const_strain_tests = Path(
    '/home/chris/Projects/MyTardisTestData/mechtests/Constant-Strain-Test-List.dat')
project_json = Path(
    '/home/chris/Projects/MyTardisTestData/mechtests/project.json')
experiment_json = Path(
    '/home/chris/Projects/MyTardisTestData/mechtests/experiment.json')
top_data_dir = Path(
    '/home/chris/Projects/MyTardisTestData/mechtests/')
global_config_file_path = Path(
    '/home/chris/Projects/MyTardisTestData/mech_global.env')

factory = MechTestFactory(local_config_file_path,
                          raw_data_dir,
                          const_strain_tests,
                          global_config_file_path)

project_dict = factory.process_project_input(project_json)
uri, project_id = factory.forge_project(project_dict)
factory.update_project(project_id,
                       project_dict)
experiment_list = factory.process_experiment_input(experiment_json)
for experiment in experiment_list:
    uri, experiment_id = factory.forge_experiment(experiment)
    response = factory.update_experiment(experiment_id,
                                         experiment)
dataset_list = factory.process_dataset_input(raw_data_dir,
                                             const_strain_tests)
for dataset in dataset_list:
    uri, dataset_id = factory.forge_dataset(dataset)
    response = factory.update_dataset(dataset_id,
                                      dataset)
file_list = factory.process_datafile_input(const_strain_tests,
                                           cyclic_data_dir,
                                           tensile_data_dir,
                                           plot_dir)
for datafile in file_list:
    factory.forge_datafile(datafile)
'''s3 = S3FileHandler(local_config_file_path)
for datafile in file_list:
    current_file = Path(datafile['local_path'])
    filepath = current_file.relative_to(top_data_dir)
    print(f'Uploading {filepath}')
    s3.upload_to_object_store(filepath)'''
