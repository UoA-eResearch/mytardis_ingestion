from . import CSVParser
import csv
from ..helper import helper as hlp
import os
import glob

class TestCSVParser(CSVParser):

    def __init__(self,
                 json_file):
        data_dict = hlp.readJSON(json_file)
        #TODO data check structure json for completeness
        self.root_dir = data_dict.pop('top_folder')
        self.tmp_dir = data_dict.pop('tmp_folder')
        self.schemas = data_dict.pop('schemas')
        self.data_dict = hlp.lowercase(data_dict)
        os.chdir(self.root_dir)
        self.csv_files = glob.glob('*.csv')
        if 'expt_id_header' in data_dict.keys():
            self.expt_id_header = data_dict['expt_id_header']
        else:
            self.expt_id_header = None
        if 'dataset_id_header' in data_dict.keys():
            self.dataset_id_heaader = data_dict['dataset_id_header']
        else:
            self.dataset_id_header = None

    def processCSVFiles(self,
                        users = None,
                        in_store=True,
                        bucket='mytardis'):
        expt_dicts = []
        dataset_dicts = []
        datafile_dicts = []
        for csv_file in self.csv_files:
            super().__init__(csv_file,
                             self.schemas,
                             self.data_dict['expt_headers'],
                             self.data_dict['expt_title_header'],
                             self.data_dict['dataset_headers'],
                             self.data_dict['dataset_description_header'],
                             self.data_dict['datafile_headers'],
                             self.data_dict['datafile_name_header'],
                             self.data_dict['rel_path_header'],
                             self.root_dir,
                             self.expt_id_header,
                             self.dataset_id_header)
            expt_dicts.append(self.create_expt_dicts(users=users))
            dataset_dicts.append(self.create_dataset_dicts())
            datafile_dicts.append(self.create_datafile_dicts(in_store=in_store,
                                                             bucket=bucket))
            return (expt_dicts, dataset_dicts, datafile_dicts)
