from . import Parser
import csv
import logging
from ..helper import check_dictionary

logger = logging.getLogger(__name__)

class CSVParser(Parser):

    def __init__(self,
                 config_dict):
        '''Initialise a CSV parser.

        Inputs:
        =================================
        config_dict: A dictionary containing the configuration details for the uploader
        
        Details of config_dict:
        =================================
        The config_dict must contain the following key/value pairs:
        
        Key / Value:
        =================================
        root_dir / the root directory with respect to the relative file paths in the csv file
        file_header / either the header or the index of the column associated with the file 
        location relative to the root_dir
        experiment_headers / a list of headers or of indices to columns associated with experiment 
        data/metadata
        dataset_headers / a list of headers or of indices to columns associated with dataset 
        data/metadata
        datafile_headers / a list of headers or of indices to columns associated with datafiles 
        data/metadata
        schema_dict / a dictionary of schema for the associated experiments/datasets/datafiles
        processed_csvs / completed csv files to be ignored if they are present in the config directory (Optional)
        processed_files / a listing of files within the current csv file that have previously been successfully
        ingested into myTardis (Optional)
        experiment_title / the header or index of the column associated with the experiment title (Optional
        - see caveats)
        internal_id / the header or index of the column associated with the internal_id (Optional - see caveats)
        dataset_description / the header or index of the column associated with the dataset description (Optional
        - see caveats)
        dataset_id / the header or index of the columns associated with the dataset_id (Optional - see caveats)
        use_headers / True or False - defines if a header row should be used to identify columns
        
        Returns:
        =================================
        Nil

        Members:
        =================================
        self.datafiles: a list of datafile dictionaries ready for fileparsing and ingestion
        self.datasets: a list of dataset dictionaries ready for ingestion
        self.experiments:  list of experiment dictionaries ready for ingestion
        self.headers: dictionary of the headers or indices of columns for different content types
        self.processed_csvs: a list of csv files that have been processed to be ignored
        self.processed_files: a list of data files that have been processed to be ignored
        self.schema_dict: the dictionary of schemas to be used to define the objects in mytardis
        self.use_headers: a boolean flag to allow csv files without headers to be used.
        '''
        self.datafiles = []
        self.datasets = []
        self.experiments = []
        self.use_headers = True
        required_keys = ['root_dir',
                         'file_header',
                         'experiment_headers'
                         'dataset_headers',
                         'datafile_headers',
                         'schema_dict']
        check = check_dictionary(config_dict,
                                 required_keys)
        if not check[0]:
            logger.error(f'Could not initialise CSV Parser due to incomplete config dictionary. Missing keys: {", ".join(check[1])}')
            raise Exception('Initalisation of the parser failed')
        else:
            if 'use_headers' in config_dict.keys():
                self.use_headers = config_dict.pop('use_headers')
            self.root_dir = config_dict['root_dir']
            self.headers = {'file': config_dict['file_header'],
                            'experiment': config_dict['experiment_headers'],
                            'dataset': config_dict['dataset_headers'],
                            'datafile': config_dict['datafile_headers']}
            self.schema_dict = config_dict['schema_dict']
            if 'processed_csvs' in config_dict.keys():
                self.processed_csvs = config_dict['processed_csvs']
            if 'processed_files' in config_dict.keys():
                self.processed_files = config_dict['processed_files']
            optional_keys = ['experiment_title',
                             'internal_id',
                             'dataset_description',
                             'dataset_id']
            for key in optional_keys:
                if key in config_dict.keys():
                    self.headers[key] = config_dict[key]

    def __get_column_indices(self,
                             headers,
                             columns):
        '''TODO document this'''
        if isinstance(headers, str):
            if not self.use_headers:
                error_message = f'The config for this parser does not read a header line and all columns must be indexed by integer. Please check config file')
                logger.error(error_message)
                raise Exception(error_message)
            else:
                headers = [headers]
        if isinstance(headers, int):
            return [headers]
        elif isinstance(headers, list):
            if all(isinstance(header, int) for header in headers):
                return headers
            else:
                if not self.use_headers:
                    error_message = f'The config for this parser does not read a header line and all columns must be indexed by integer. Please check config file')
                    logger.error(error_message)
                    raise Exception(error_message)
                else:
                    headers_ind = []
                    for header in headers:
                        if isinstance(header, int):
                            headers_ind.append(header)
                        else:
                            header = str(header).lower() # Make it case insensitive by forcing lowercase
                            flg = False
                            for column in colums:
                                if column.lower().strip() == header.strip():
                                    if flg:
                                        error_message = f'Cannot determine column indices from header due to duplicate entries' 
                                        logger.error(error_message)
                                        raise Exception(error_message)
                                    else:
                                        headers_ind.append(columns.index(column))
                return headers_ind
        else:
            error_message = f'Cannot parse header of type {type(headers)}'
            logger.error(error_message)
            raise Exception(error_message)

    def __build_experiment_index_dict(self,
                                      columns):
        expt_inds = {}
        expt_meta = {}
        if 'experiment_title' in self.headers:
            expt_inds['experiment_title'] = self.__get_column_indices(self.headers['experiment_title'],
                                                                      columns)
        else:
            # Here be dragons!
            # Because the experiment must have a title in order to be properly formed
            # a default of the first item in the experiment header listing will be used.
            # This will have obvious consequences should the first item be metadata rather than
            # the experiment name, but does serve a short cut for carefully constructed config files.
            # This can be done with relative safety since the data is stored in lists and thus has
            # a predefined ordering.
            self.headers['experiment_title'] = self.headers['experiment_headers'][0]
            expt_inds['experiment_title'] = self.__get_column_indices(self.headers['experiment_headers']pop(0),
                                                                      columns)
        if 'internal_id' in self.headers:
            expt_inds['internal_id'] = self.__get_column_indices(self.headers['internal_id'],
                                                                 columns)
        else:
            # More dragons here and arguably bigger and scarier ones than above!
            # Experiments must also have an internal_id in order to be properly formed.
            # A default value of the experiment_title can be used, but this relies on every experiment
            # being uniquely named, which will inevitably fall over at some point.
            # Use this approach as a means of last resort.
            expt_inds['internal_id'] = self.__get_column_indices(expt_inds['experiment_title'],
                                                                 columns)
        for key in headers['experiment_headers']:
            expt_meta[f'Experiment_{str(key).lower().replace(" ", "_")}'] = self.__get_column_indices(key,
                                                                                                      columns)
        expt_inds['meta'] = expt_meta
        return expt_inds
            
    def __build_experiment_dictionary(self,
                                      row,
                                      expt_inds):
        '''TODO Document this'''
        expt_dict = {}
        if len(expt_inds['experiment_title']) != 1 or len(expt_inds['internal_id']) != 1:
            error_message = f'Malformed experiment_title list'
            logger.warning(error_message)
            raise Exception(error_message)
        expt_dict['title'] = row[expt_inds['experiment_title'][0]]
        expt_dict['internal_id'] = row[expt_inds['internal_id'][0]]
        if expt_inds['meta'] not {}:
            for key in expt_inds['meta'].keys():
                expt_dict[key] = row[expt_inds['meta'][key][0]]
        return expt_dict
            

    def process_file(self,
                     csvfile):
        with open(csvfile, newline='') as f:
            reader = csv.reader(csvfile, delimiter=self.delimiter, quotechar='"')
            header = next(reader)
            try:
                expt_inds = self.__build_experiment_index_dict(header)
            
