from Core import IngestionFactory
from Core.helpers import readJSON, calculate_md5sum, process_config
from Core.helpers import RAiDFactory
from pathlib import Path
import mimetypes
import json


class PGPIngestFactory(IngestionFactory):

    def __init__(self,
                 local_config_file_path,
                 global_config_file_path,
                 project_json_path,
                 experiment_json_path,
                 dataset_json_path):
        super().__init__(local_config_file_path)
        local_keys = ['remote_root',
                      'staging_root']
        self.config = process_config(keys=local_keys,
                                     local_filepath=local_config_file_path)
        self.project_default_url = 'https://mytardis.nectar.auckland.ac.nz/project/'
        self.experiment_default_url = 'https://mytardis.nectar.auckland.ac.nz/experiment/'
        self.dataset_default_url = 'https://mytardis.nectar.auckland.ac.nz/dataset/'
        self.datafile_default_url = 'https://mytardis.nectar.auckland.ac.nz/datafile/'
        self.raid_factory = RAiDFactory(global_config_file_path)
        self.project_json_path = project_json_path
        self.experiment_json_path = experiment_json_path
        self.dataset_json_path = dataset_json_path

    def process_project_input(self):
        project_dict = readJSON(self.project_json_path)
        project_list = []
        schema = project_dict['schema']
        admin_groups = project_dict['admin_groups']
        admins = project_dict['admins']
        for project in project_dict['projects']:
            project['schema'] = schema
            project['admin_groups'] = admin_groups
            project['admins'] = admins
            project_list.append(project)
        return project_list

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

    def process_experiment_input(self):
        experiment_dict = readJSON(self.experiment_json_path)
        experiment_list = []
        schema = experiment_dict['schema']
        admin_groups = experiment_dict['admin_groups']
        admins = experiment_dict['admins']
        for experiment in experiment_dict['experiments']:
            experiment['project'] = experiment.pop('project_id')
            if 'schema' not in experiment.keys():
                experiment['schema'] = schema
            if 'admin_groups' not in experiment.keys():
                experiment['admin_groups'] = admin_groups
            if 'admins' not in experiment.keys():
                experiment['admins'] = admins
            if 'description' not in experiment.keys():
                experiment['description'] = "No description"
            experiment_list.append(experiment)
        return experiment_list

    def update_experiment(self,
                          experiment_id,
                          experiment_dict):
        raid = experiment_dict['raid']
        print(self.experiment_default_url +
              str(experiment_id))
        response = self.raid_factory.update_raid(str(experiment_dict["title"]),
                                                 'PGG Sample Barcode',
                                                 self.experiment_default_url +
                                                 str(experiment_id),
                                                 raid)
        return response

    def process_datafile_input(self):
        pass

    def process_dataset_input(self):
        dataset_dict = readJSON(self.dataset_json_path)
        print(dataset_dict)
        dataset_list = []
        schema = dataset_dict['schema']
        admin_groups = dataset_dict['admin_groups']
        admins = dataset_dict['admins']
        for dataset in dataset_dict['datasets']:
            dataset['experiments'] = [dataset.pop('experiments')]
            if 'schema' not in dataset.keys():
                dataset['schema'] = schema
            if 'admin_groups' not in dataset.keys():
                dataset['admin_groups'] = admin_groups
            if 'admins' not in dataset.keys():
                dataset['admins'] = admins
            dataset_list.append(dataset)
        return dataset_list

    def update_dataset(self,
                       dataset_id,
                       dataset_dict):
        raid = dataset_dict['dataset_id']
        print(self.dataset_default_url +
              str(dataset_id))
        response = self.raid_factory.update_raid(str(dataset_dict["description"]),
                                                 'PGG Sample',
                                                 self.dataset_default_url +
                                                 str(dataset_id),
                                                 raid)
