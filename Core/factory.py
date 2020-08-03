# factory.py
#
# Abstract Ingestion Factory class
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 23 Jul 2020
#

from abc import ABC, abstractmethod
from .overseer import Overseer


class IngestionFactory(ABC):

    def __init__(self,
                 local_config_file_path):
        self.local_config_file_path = local_config_file_path
        self.overseer = Overseer(local_config_file_path)

    @abstractmethod
    def process_project_input(self):
        pass

    @abstractmethod
    def process_experiment_input(self):
        pass

    @abstractmethod
    def process_dataset_input(self):
        pass

    @abstractmethod
    def process_datafile_input(self):
        pass

    def forge_project(self,
                      input_dict):
        print('Forging Project')
        from .forge import ProjectForge
        forge = ProjectForge(self.local_config_file_path)
        print(forge)
        try:
            print(input_dict)
            uri, project_id = forge.get_or_create(input_dict)
        except Exception as error:
            raise error
        return (uri, project_id)

    def forge_experiment(self,
                         input_dict):
        from .forge import ExperimentForge
        forge = ExperimentForge(self.local_config_file_path)
        try:
            uri, experiment_id = forge.get_or_create(input_dict)
        except Exception as error:
            raise error
        return (uri, experiment_id)

    def forge_dataset(self,
                      input_dict):
        from .forge import DatasetForge
        forge = DatasetForge(self.local_config_file_path)
        try:
            uri, dataset_id = forge.get_or_create(input_dict)
        except Exception as error:
            raise error
        return (uri, dataset_id)

    def forge_datafile(self,
                       input_dict):
        from .forge import DatafileForge
        forge = DatafileForge(self.local_config_file_path)
        try:
            uri, datafile_id = forge.get_or_create(input_dict)
        except Exception as error:
            raise error
        return (uri, datafile_id)
