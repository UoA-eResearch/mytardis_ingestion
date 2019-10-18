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
from pathlib import Path

logger = logging.getLogger

class DirParser(Parser, ABC):

    @abstractmethod
    def __init__(self,
                 config_dict,
                 harvester):
        super().__init__(config_dict, harvester)

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

    def build_dir_list(self,
                       directory = None):
        if not directory:
            directory = self.root_dir
        subdirectories = []
        for dirname, subdirs, files in os.walk(directory):
            cur_path = Path(dirname)
            subdirectories.append(cur_path)
        return subdirectories
