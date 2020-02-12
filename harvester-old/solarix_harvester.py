from . import Harvester
import logging
from .parsers import SolarixParser
from .filehandlers import SolarixFileHandler
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

class SolarixHarvester(Harvester):

    def __init__(self,
                 config_dir,
                 filepath):
        self.processed_list = self.read_processed_file_list(filepath)
        super().__init__(config_dir)
        self.files_dict = {}
        self.processed_file_path = filepath
        
    def read_processed_file_list(self,
                                 filepath):
        processed_list = []
        if os.path.isfile(filepath):
            with open(filepath, 'r') as f:
                for line in f:
                    processed_list.append(Path(line.strip('\n')))
        return processed_list
        
    def parser(self,
               config_dict,
               harvester):
        try:
            parser = SolarixParser(config_dict, harvester)
        except Exception as err:
            logger.critical(f'Shutting down harvester while initialising Parser, Error {err}')
            sys.exit()
        return parser

    def filehandler(self,
                    config_dict,
                    harvester):
        try:
            filehandler = SolarixFileHandler(config_dict, harvester)
        except Exception as err:
            logger.critical(f'Shutting down harvester while initialising File handler, Error {err}')
            sys.exit()
        return filehandler

    def harvest(self):
        for key in self.parser.projects.keys():
            if self.parser.projects[key][8] != []:
                self.filehandler.create_method_tar_file(self.parser.projects[key][8][0])
        experiments = self.parser.create_experiment_dicts()
        datasets = self.parser.create_dataset_dicts()
        datafiles = self.parser.create_datafile_dicts()
        for experiment in experiments:
            self.ingestor.create_experiment(experiments[experiment])
        for dataset in datasets:
            self.ingestor.create_dataset(datasets[dataset])
        for datafile in datafiles:
            self.filehandler.upload_file(datafile)
            self.ingestor.create_datafile(datafile)
