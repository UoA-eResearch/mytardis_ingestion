from . import Parser
import csv
import logging
from ..helper import check_dictionary
from ..helper import calculate_checksum, md5_python, md5_subprocess

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
        file_header / either the header or the index of the column associated with the file name
        local_dir /  either the header or the index of the column associated with the file location relative
        to the root_dir
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
        self.delimiter = ','
        required_keys = ['root_dir',
                         'file_header',
                         'local_dir',
                         'experiment_headers',
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
                            'local_dir': config_dict['local_dir'],
                            'experiment_headers': config_dict['experiment_headers'],
                            'dataset_headers': config_dict['dataset_headers'],
                            'datafile_headers': config_dict['datafile_headers']}
            self.schema_dict = config_dict['schema_dict']
            if 'processed_csvs' in config_dict.keys():
                self.processed_csvs = config_dict['processed_csvs']
            if 'processed_files' in config_dict.keys():
                self.processed_files = config_dict['processed_files']
            if 'delimiter' in config_dict.keys():
                self.delimiter = config_dict['delimiter']
            optional_keys = ['experiment_title',
                             'internal_id',
                             'dataset_description',
                             'dataset_id',
                             'remote_dir']
            for key in optional_keys:
                if key in config_dict.keys():
                    self.headers[key] = config_dict[key]

    def __get_column_indices(self,
                             headers,
                             columns):
        '''TODO document this'''
        logger.debug(columns)
        if isinstance(headers, str):
            if not self.use_headers:
                error_message = f'The config for this parser does not read a header line and all columns must be indexed by integer. Please check config file'
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
                    error_message = f'The config for this parser does not read a header line and all columns must be indexed by integer. Please check config file'
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
                            for column in columns:
                                logger.debug(column.lower().strip() == header.strip())
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
        '''TODO Document this'''
        expt_inds = {}
        expt_meta = {}
        if 'experiment_title' in self.headers:
            expt_inds['experiment_title'] =\
                self.__get_column_indices(self.headers['experiment_title'],
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
            expt_inds['experiment_title'] =\
                self.__get_column_indices(self.headers['experiment_headers'].pop(0),
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
        for key in self.headers['experiment_headers']:
            expt_meta[f'Experiment_{str(key).lower().replace(" ", "_")}'] =\
                self.__get_column_indices(key,
                                          columns)
        expt_inds['meta'] = expt_meta
        return expt_inds

    def __build_dataset_index_dict(self,
                                   columns):
        '''TODO Document this'''
        dataset_inds = {}
        dataset_meta = {}
        if 'dataset_description' in self.headers:
            dataset_inds['dataset_description'] =\
                self.__get_column_indices(self.headers['dataset_description'],
                                          columns)
        else:
            # Here be dragons!
            # Because the dataset must have a description in order to be properly formed
            # a default of the first item in the dataset header listing will be used.
            # This will have obvious consequences should the first item be metadata rather than
            # the dataset description, but does serve a short cut for carefully constructed config files.
            # This can be done with relative safety since the data is stored in lists and thus has
            # a predefined ordering.
            self.headers['dataset_description'] = self.headers['dataset_headers'][0]
            dataset_inds['dataset_description'] =\
                self.__get_column_indices(self.headers['dataset_headers'].pop(0),
                                          columns)
        if 'dataset_id' in self.headers:
            dataset_inds['dataset_id'] =\
                self.__get_column_indices(self.headers['dataset_id'],
                                          columns)
        else:
            # More dragons here and arguably bigger and scarier ones than above!
            # Datasets must also have an dataset_id in order to be properly formed.
            # A default value of a composite based on the experiment_titile and there
            # dataset_description can be used, but this relies on every experiment/dataset
            # being uniquely named, which will inevitably fall over at some point.
            # Use this approach as a means of last resort.
            dataset_inds['dataset_id'] =\
                self.__get_column_indices(expt_inds['dataset_description'],
                                          columns)
        if 'internal_id' in self.headers:
            dataset_inds['internal_id'] =\
                self.__get_column_indices(self.headers['internal_id'],
                                          columns)
        else:
            # More dragons here and arguably bigger and scarier ones than above!
            # Experiments must also have an internal_id in order to be properly formed.
            # A default value of the experiment_title can be used, but this relies on every experiment
            # being uniquely named, which will inevitably fall over at some point.
            # Use this approach as a means of last resort.
            dataset_inds['internal_id'] =\
                self.__get_column_indices(self.headers['experiment_title'],
                                          columns)
        for key in self.headers['dataset_headers']:
            dataset_meta[f'Dataset_{str(key).lower().replace(" ", "_")}'] =\
                self.__get_column_indices(key,
                                          columns)
        dataset_inds['meta'] = dataset_meta
        return dataset_inds

    def __build_datafile_index_dict(self,
                                    columns):
        datafile_inds = {}
        datafile_meta = {}
        datafile_inds['file'] =\
            self.__get_column_indices(self.headers['file'],
                                      columns)
        # This should have been defined eariler as it is not possible to attach the data file to a non-existant
        # dataset
        datafile_inds['dataset_id'] =\
            self.__get_column_indices(self.headers['dataset_id'],
                                      columns)
        datafile_inds['local_dir'] =\
            self.__get_column_indices(self.headers['local_dir'],
                                      columns)
        if 'remote_dir' in self.headers.keys():
            datafile_inds['remote_dir'] =\
                self.__get_column_indices(self.headers['remote_dir'],
                                          columns)
        for key in self.headers['datafile_headers']:
            datafile_meta[f'Datafile_{str(key).lower().replace(" ", "_")}'] =\
                self.__get_column_indices(key,
                                          columns)
        datafile_inds['meta'] = datafile_meta
        return datafile_inds

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
        expt_dict['schema_namespace'] = self.schema_dict['experiment']
        if expt_inds['meta'] != {}:
            for key in expt_inds['meta'].keys():
                expt_dict[key] = row[expt_inds['meta'][key][0]]
        return expt_dict

    def __build_dataset_dictionary(self,
                                   row,
                                   dataset_inds):
        '''TODO Document this'''
        dataset_dict = {}
        if len(dataset_inds['dataset_description']) != 1 or len(dataset_inds['dataset_id']) != 1:
            error_message = f'Malformed dataset_description list'
            logger.warning(error_message)
            raise Exception(error_message)
        dataset_dict['description'] = row[dataset_inds['dataset_description'][0]]
        dataset_dict['internal_id'] = row[dataset_inds['internal_id'][0]]
        dataset_dict['dataset_id'] = row[dataset_inds['dataset_id'][0]]
        dataset_dict['schema_namespace'] = self.schema_dict['dataset']
        if dataset_inds['meta'] != {}:
            for key in dataset_inds['meta'].keys():
                dataset_dict[key] = row[dataset_inds['meta'][key][0]]
        return dataset_dict

    def __build_datafile_dictionary(self,
                                    row,
                                    datafile_inds):
        datafile_dict = {}
        if len(datafile_inds['file']) != 1 or len(datafile_inds['dataset_id']) != 1\
           or len (datafile_inds['local_dir']) != 1:
            error_message = f'Malformed datafile_ind dictionary'
            logger.warning(error_message)
            raise Exception(error_message)
        datafile_dict['file'] = row[datafile_inds['file'][0]]
        datafile_dict['local_dir'] = row[datafile_inds['local_dir'][0]]
        datafile_dict['dataset_id'] = row[datafile_inds['dataset_id'][0]]
        if 'remote_dir' in datafile_inds.keys():
            datafile_dict['remote_dir'] = row[datafile_inds['remote_dir'][0]]
        else: # Mirror local directory structure unless otherwise informed
            datafile_dict['remote_dir'] = row[datafile_inds['local_dir'][0]]
        if datafile_inds['meta'] != {}:
            for key in datafile_inds['meta'].keys():
                datafile_dict[key] = row[datafile_inds['meta'][key][0]]
        return datafile_dict
    
    def create_experiment_dicts(self,
                               csvfile,
                               force = False):
        if self.experiments == [] or force is True:
            processed = self.process_file(csvfile)
        return self.experiments

    def create_dataset_dicts(self,
                             csvfile,
                             force = False):
        if self.datasets == [] or force is True:
            processed = self.process_file(csvfile)
        return self.datasets

    def create_datafile_dicts(self,
                              csvfile,
                              force = False):
        if self.datafiles == [] or force is True:
            processed = self.process_file(csvfile)
        return self.datafiles

    def __compile_experiment_metadata(self,
                                      experiment_list):
        internal_ids = []
        return_list = []
        for expt in experiment_list:
            if expt['internal_id'] in internal_ids:
                index = internal_ids.index(expt['internal_id'])
                compiled_dict = return_list[index]
                if expt['title'] != compiled_dict['title']:
                    logger.warning(f'Two different experiment titles {compiled_dict["title"]} and {expt["title"]} have been found for the same internal_id, {expt["internal_id"]}. Using {compiled_dict["title"]}.')
                for key in expt.keys():
                    if key == 'title' or key == 'internal_id':
                        continue # do nothing since these have already been handled
                    else:
                        if key in compiled_dict.keys():
                            if expt[key] not in compiled_dict[key]:
                                compiled_dict[key].append(expt[key])
                        else:
                            compiled_dict[key] = [expt[key]]
            else:
                internal_ids.append(expt['internal_id'])
                compiled_dict = {}
                for key in expt.keys():
                    if key == 'title' or key == 'internal_id':
                        compiled_dict[key] = expt[key]
                    else:
                        compiled_dict[key] = [expt[key]]
            return_list.append(compiled_dict)
        return return_list
    
    def __compile_dataset_metadata(self,
                                   dataset_list):
        dataset_composite_ids = []
        return_list = []
        for dataset in dataset_list:
            composite_id = f'{dataset["internal_id"]}-{dataset["dataset_id"]}'
            if composite_id in dataset_composite_ids:
                index = dataset_composite_ids.index(composite_id)
                compiled_dict = return_list[index]
                if dataset['description'] != compiled_dict['description']:
                    logger.warning(f'Two different dataset descriptions {compiled_dict["description"]} and {dataset["description"]} have been found for the same composite_id, {composite_id}. Using {compiled_dict["description"]}.')
                for key in dataset.keys():
                    if key == 'description' or key == 'internal_id' or key == 'dataset_id':
                        continue # do nothing since these have already been handled
                    else:
                        if key in compiled_dict.keys():
                            if dataset[key] not in compiled_dict[key]:
                                compiled_dict[key].append(dataset[key])
                        else:
                            compiled_dict[key] = [dataset[key]]
            else:
                dataset_composite_ids.append(composite_id)
                compiled_dict = {}
                for key in dataset.keys():
                    if key == 'description' or key == 'internal_id' or key == 'dataset_id':
                        compiled_dict[key] = dataset[key]
                    else:
                        compiled_dict[key] = [dataset[key]]
            return_list.append(compiled_dict)
        return return_list    
    
    def process_file(self,
                     csvfile):
        with open(csvfile, newline='') as f:
            reader = csv.reader(f, delimiter=self.delimiter, quotechar='"')
            header = next(reader)
            try:
                expt_inds = self.__build_experiment_index_dict(header)
            except Exception as err:
                error_message = f'Unable to build the experiment index dictionary due to error, {err}'
                logger.error(error_message)
                raise err
            try:
                dataset_inds = self.__build_dataset_index_dict(header)
            except Exception as err:
                error_message = f'Unable to build the dataset index dictionary due to error, {err}'
                logger.error(error_message)
                raise err
            try:
                datafile_inds = self.__build_datafile_index_dict(header)
            except Exception as err:
                error_message = f'Unable to build the datafile index dictionary due to error, {err}'
                logger.error(error_message)
                raise err
            for row in reader:
                expt = self.__build_experiment_dictionary(row,
                                                          expt_inds)
                if expt not in self.experiments:
                    self.experiments.append(expt)
                dataset = self.__build_dataset_dictionary(row,
                                                          dataset_inds)
                if dataset not in self.datasets:
                    self.datasets.append(dataset)
                datafile = self.__build_datafile_dictionary(row,
                                                            datafile_inds)
                if datafile not in self.datafiles:
                    self.datafiles.append(datafile)
        self.experiments = self.__compile_experiment_metadata(self.experiments)
        self.datasets = self.__compile_dataset_metadata(self.datasets)
        return True
