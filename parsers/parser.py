# Abstract Parser class as a base for the specialised Parsers needed
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#

__author__ = "Chris Seal <c.seal@auckland.ac.nz>"

from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)

class Parser(ABC):

    @abstractmethod
    def  __init__(self,
                  config_dict):
        pass

    @abstractmethod
    def get_file_list(self):
        pass
    
    @abstractmethod
    def create_datafile_dicts(self):
        pass

    @abstractmethod
    def create_dataset_dicts(self):
        pass

    @abstractmethod
    def create_experiment_dicts(self):
        pass
