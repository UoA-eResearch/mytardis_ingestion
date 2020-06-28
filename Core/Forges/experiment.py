# Class to ingest project data into MyTardis

from abc import ABC, abstractmethod
from foundry import MyTardisFoundry
from rest import MyTardisREST

import logging

logger = logging.getLogger(__name__)

class ExperimentFoundry(ABC, MyTardisFoundry):

    def __init__(self,
                 required_keys,
                 server,
                 username,
                 api_key,
                 proxies = None,
                 verify_certificate=True):
        super().__init__(required_keys,
                         server,
                         username,
                         api_key,
                         proxies = None,
                         verify_certificate=True)
        

    @abstractmethod
    def process_input():
        pass

    def get_experiment_by_raid(raid):
        pass

    def attach_parameters():
        pass

class ExperimentForge(MyTardisREST):

    def __init__(self,
                 global_config_file_path,
                 local_config_file_path,
                 checksum_digest=None):
        
