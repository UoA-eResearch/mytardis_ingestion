# mech_test.py
#
# Implementation of the IngestionFactory for testing ingestion using existing mechanical test data
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 06 Aug 2020
#

from Core import IngestionFactory
from Core.helpers import readJSON, calculate_md5sum, process_config
from pathlib import Path
import mimetypes
from Core.helpers import RAiDFactory
import json


def filename_builder(original_name):
    test_number = original_name.split('-')[1]
    raw_cyclic = f'test{test_number}.dat'
    raw_tensile = f'test{test_number}t.dat'
    processed_tensile = f'Test-{test_number}.dat'
    processed_cyclic = f'Test-{test_number}.dat'
    return (raw_cyclic, raw_tensile, processed_tensile, processed_cyclic)


def parse_header(filename):
    header = {}
    with open(filename, 'r') as f:
        count = 0
        for line in f:
            count += 1
            # print(count)
            if line.strip().startswith('Procedure Name:'):
                data = line.split(':')
                header['dataset_procedure'] = data[1].strip()
                continue
            if line.strip().startswith('File Specification'):
                data = line.split('=')
                header['dataset_procedure_file'] = data[1].strip()
                continue
            if line.strip().startswith('Level Increment'):
                data = line.split('=')
                header['dataset_increment'] = data[1].strip()
                continue
            if line.strip().startswith('Rate'):
                data = line.split('=')
                header['dataset_load_rate'] = data[1].strip()
                continue
            if line.strip().startswith('Control Mode'):
                data = line.split('=')
                header['dataset_control_mode'] = data[1].strip()
                continue
            if line.strip().startswith('Mode'):
                data = line.split('=')
                header['dataset_mode'] = data[1].strip()
                continue
            if line.strip().startswith('Master Channel'):
                data = line.split('=')
                header['dataset_master'] = data[1].strip()
                continue
            if line.strip().startswith('Slave Channel 1'):
                data = line.split('=')
                header['dataset_slave1'] = data[1].strip()
                continue
            if line.strip().startswith('Slave Channel 2'):
                data = line.split('=')
                header['dataset_slave2'] = data[1].strip()
                continue
            if line.strip().startswith('Slave Channel 3'):
                data = line.split('=')
                header['dataset_slave3'] = data[1].strip()
                continue
            if line.strip().startswith('Slave Channel 4'):
                data = line.split('=')
                header['dataset_slave4'] = data[1].strip()
                continue
    return header


def get_filenames(filename):
    files = {}
    with open(filename, 'r') as f:
        for line in f:
            if line.startswith('Test'):
                data = line.split('\t')
                cyclic, tensile, processed_tensile, processed_cyclic = filename_builder(
                    data[0])
                files[tensile] = (
                    cyclic, tensile, processed_tensile, processed_cyclic)
    return files


def extract_const(filename):
    combo_dict = {}
    with open(filename, 'r') as f:
        for line in f:
            if line.startswith('Test'):
                data = line.split('\t')
                _, tensile, _, _ = filename_builder(data[0])
                key1 = round(100*float(data[1]), 2)
                if key1 not in combo_dict.keys():
                    combo_dict[key1] = {}
                key2 = int(data[2].strip())
                if key2 not in combo_dict[key1].keys():
                    combo_dict[key1][key2] = []
                combo_dict[key1][key2].append(tensile)
    return combo_dict


class MechTestFactory(IngestionFactory):

    def __init__(self,
                 local_config_file_path,
                 raw_data_dir,
                 test_list_file_path,
                 global_config_file_path):
        super().__init__(local_config_file_path)
        local_keys = ['remote_root',
                      'staging_root']
        self.config = process_config(keys=local_keys,
                                     local_filepath=local_config_file_path)
        self.raw_data_dir = Path(raw_data_dir)
        self.test_list_file_path = Path(test_list_file_path)
        self.key1 = []
        self.key2 = {}
        self.raid_factory = RAiDFactory(global_config_file_path)
        self.raid_list = self.get_all_raids()
        self.project_default_url = 'https://mytardis.nectar.auckland.ac.nz/project/'
        self.experiment_default_url = 'https://mytardis.nectar.auckland.ac.nz/experiment/'
        self.dataset_default_url = 'https://mytardis.nectar.auckland.ac.nz/dataset/'
        self.datafile_default_url = 'https://mytardis.nectar.auckland.ac.nz/datafile/'
        self.project_id = ""
        self.experiment_raids = {}
        self.dataset_raids = {}

    def get_all_raids(self):
        ret_list = []
        response = self.raid_factory.get_all_raids()
        resp_dict = json.loads(response.text)
        raids = resp_dict['items']
        for raid in raids:
            ret_list.append(raid['handle'])
        return ret_list

   ''' def reuse_RAiDs(self,
                    name,
                    description,
                    url):
         Given that this script is used for testing purposes
        we will re-use raids for existing list prior to minting
        new ones.
        if len(self.raid_list) < 1:
            response = self.raid_factory.mint_raid(name,
                                                   description,
                                                   url)
            resp_dict = json.loads(response.text)
            raid = resp_dict["handle"]
        else:
            raid = self.raid_list.pop()
        return raid'''

    def process_project_input(self,
                              project_json):
        project_dict = readJSON(project_json)
        project_dict["raid"] = self.reuse_RAiDs(project_dict["name"],
                                                project_dict["description"],
                                                self.project_default_url)
        self.project_id = project_dict['raid']
        return project_dict

    def process_experiment_input(self,
                                 experiment_json):
        return_list = []
        unprocessed_dict = readJSON(experiment_json)
        experiments = unprocessed_dict['experiments']
        unprocessed_dict['project_id'] = self.project_id
        for experiment in experiments:
            experiment['schema'] = unprocessed_dict['schema']
            experiment['project'] = unprocessed_dict['project_id']
            old_raid = experiment['raid']
            experiment['raid'] = self.reuse_RAiDs(experiment['title'],
                                                  experiment['description'],
                                                  self.experiment_default_url)
            self.experiment_raids[old_raid] = experiment['raid']
            return_list.append(experiment)
        return return_list

    def process_dataset_input(self,
                              raw_data_directory,
                              const_strain_datfile):
        tests = extract_const(const_strain_datfile)
        print(tests)
        datasets = []
        for key1 in tests.keys():
            if key1 == 5.0:
                experiment = self.experiment_raids['Const-5']
            elif key1 == 2.5:
                experiment = self.experiment_raids['Const-25']
            else:
                experiment = self.experiment_raids['Const-125']
            for key2 in tests[key1].keys():
                dataset_prestrain = key2
                dataset_id = self.reuse_RAiDs(f'{key2} load cycles at +/-{key1} %',
                                              'Mechanical test dataset',
                                              self.dataset_default_url)
                self.dataset_raids[f'MechDataSet{key1}/{key2}'] = dataset_id
                test_file_name = raw_data_directory / tests[key1][key2][0]
                dataset = parse_header(test_file_name)
                dataset['dataset_prestrain'] = dataset_prestrain
                dataset['experiments'] = [experiment]
                dataset['instrument_id'] = 'TEST-MTS-1'
                dataset['dataset_id'] = dataset_id
                dataset['description'] = f'{key2} load cycles at +/-{key1} %'
                dataset['schema'] = 'http://test.mytardis.nectar.auckland.ac.nz/FoE/Mechanical-test/MTS/v1/Dataset'
                datasets.append(dataset)
        return datasets

    def process_datafile_input(self,
                               const_strain_datfile,
                               cyc_data_dir,
                               tens_data_dir,
                               plot_dir):
        files = get_filenames(const_strain_datfile)
        tests = extract_const(const_strain_datfile)
        datafiles = []
        for key1 in tests.keys():
            for key2 in tests[key1].keys():
                dataset_id = self.dataset_raids[f'MechDataSet{key1}/{key2}']
                datafile_base = {'dataset_id': dataset_id,
                                 'storage_box': 'TEST-mechanical-tests',
                                 'schema': 'http://test.mytardis.nectar.auckland.ac.nz/FoE/Mechanical-test/MTS/v1/Datafile'}
                for test_file in tests[key1][key2]:
                    datafile_tensile_in = {}
                    datafile_tensile_in.update(datafile_base)
                    datafile_cyclic_in = {}
                    datafile_cyclic_in.update(datafile_base)
                    datafile_tensile_plt = {}
                    datafile_tensile_plt.update(datafile_base)
                    datafile_cyclic_plt = {}
                    datafile_cyclic_plt.update(datafile_base)
                    _, _, tensile, cyclic = files[test_file]
                    tensile_plt = tensile.split('.')[0] + '.png'
                    cyclic_plt = cyclic.split('.')[0] + '_cyc.png'
                    tensile_plot = plot_dir / tensile_plt
                    cyclic_plot = plot_dir / cyclic_plt
                    tensile_input = tens_data_dir / tensile
                    cyclic_input = cyc_data_dir / cyclic
                    datafile_tensile_in['filename'] = tensile
                    datafile_tensile_in['local_path'] = tensile_input.as_posix(
                    )
                    datafile_tensile_in['remote_path'] = tensile_input.relative_to(
                        self.config['staging_root']).as_posix()
                    datafile_tensile_in['md5sum'] = calculate_md5sum(
                        tensile_input)
                    datafile_tensile_in['mimetype'] = mimetypes.guess_type(
                        tensile_input)
                    datafile_tensile_in['size'] = tensile_input.stat().st_size
                    datafile_tensile_in['full_path'] = (
                        self.config['remote_root'] / tensile_input.relative_to(self.config['staging_root'])).as_posix()
                    datafile_cyclic_in['filename'] = cyclic
                    datafile_cyclic_in['local_path'] = cyclic_input.as_posix()
                    datafile_cyclic_in['remote_path'] = cyclic_input.relative_to(
                        self.config['staging_root']).as_posix()
                    datafile_cyclic_in['md5sum'] = calculate_md5sum(
                        cyclic_input)
                    datafile_cyclic_in['mimetype'] = mimetypes.guess_type(
                        cyclic_input)
                    datafile_cyclic_in['size'] = cyclic_input.stat().st_size
                    datafile_cyclic_in['full_path'] = (
                        self.config['remote_root'] / cyclic_input.relative_to(self.config['staging_root'])).as_posix()
                    datafile_tensile_plt['filename'] = tensile_plt
                    datafile_tensile_plt['local_path'] = tensile_plot.as_posix(
                    )
                    datafile_tensile_plt['remote_path'] = tensile_plot.relative_to(
                        self.config['staging_root']).as_posix()
                    datafile_tensile_plt['md5sum'] = calculate_md5sum(
                        tensile_plot)
                    datafile_tensile_plt['mimetype'] = mimetypes.guess_type(
                        tensile_plot)
                    datafile_tensile_plt['size'] = tensile_plot.stat().st_size
                    datafile_tensile_plt['full_path'] = (
                        self.config['remote_root'] / tensile_plot.relative_to(self.config['staging_root'])).as_posix()
                    datafile_cyclic_plt['filename'] = cyclic_plt
                    datafile_cyclic_plt['local_path'] = cyclic_plot.as_posix()
                    datafile_cyclic_plt['remote_path'] = cyclic_plot.relative_to(
                        self.config['staging_root']).as_posix()
                    datafile_cyclic_plt['md5sum'] = calculate_md5sum(
                        cyclic_plot)
                    datafile_cyclic_plt['mimetype'] = mimetypes.guess_type(
                        cyclic_plot)
                    datafile_cyclic_plt['size'] = cyclic_plot.stat().st_size
                    datafile_cyclic_plt['full_path'] = (
                        self.config['remote_root'] / cyclic_plot.relative_to(self.config['staging_root'])).as_posix()
                    datafiles.append(datafile_tensile_in)
                    datafiles.append(datafile_cyclic_in)
                    datafiles.append(datafile_tensile_plt)
                    datafiles.append(datafile_cyclic_plt)
        return datafiles

    def update_project(self,
                       project_id,
                       project_dict):
        raid = project_dict['raid']
        response = self.raid_factory.update_raid(project_dict["name"],
                                                 project_dict["description"],
                                                 self.project_default_url +
                                                 str(project_id),
                                                 raid)
        return response

    def update_experiment(self,
                          experiment_id,
                          experiment_dict):
        raid = experiment_dict['raid']
        response = self.raid_factory.update_raid(experiment_dict["title"],
                                                 experiment_dict["description"],
                                                 self.experiment_default_url +
                                                 str(experiment_id),
                                                 raid)
        return response

    def update_dataset(self,
                       dataset_id,
                       dataset_dict):
        raid = dataset_dict['dataset_id']
        response = self.raid_factory.update_raid(dataset_dict["description"],
                                                 'Mechanical test dataset',
                                                 self.dataset_default_url +
                                                 str(dataset_id),
                                                 raid)
        return response
