# Abstract Directory Parser class as a base for the specialised Parsers needed
#
# Provides basic structure details from the directory structure
# Abstract class as it must have the metadata attached some how
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#

__author__ = "Chris Seal <c.seal@auckland.ac.nz>"

from .parser import Parser
from abc import ABC, abstractmethod
import logging
import os

logger = logging.getLogger

class DirParser(Parser, ABC):

    @abstractmethod
    def __init__(self,
                 config_dict):
        self.root_dir = config_dict['root_dir']

    @abstractmethod
    def create_datafile_dicts(self):
        pass

    @abstractmethod
    def create_dataset_dicts(self):
        pass

    @abstractmethod
    def create_experiment_dicts(self):
        pass

    
#TODO abstract out some of the core functionality of the DIR parser here
