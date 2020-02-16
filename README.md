# myTardis Ingestion
Ingestion scripts to automate creation and data incorporation for UoA iteration of myTardis

# uploader.py
Uploader is the main ingestion script that interacts with myTardis.

**NOTE: This is old and out of date - new documentation is indevelopment**

## MtUploader class:
The MtUploader class provides a means of accessing myTardis via an api key that each instance of the class is initiated with. The class has the following functions:
### Private functions:
* __\_\_init\_\___(self, config_file) - reads in a [config](###Config:) file in JSON format and initialises class.
* __\_\_check_config_dictionary(self, config_dict)__ - carry out basic sanity check on the config to ensure that the dictionary is complete to enable the class to properly initialise.
* __\_\_json_request_headers(self)__ - return the standard request headers in a JSON format ready to be pushed to the myTardis REST API
* __\_\_raise_request_exception(self, response)__ - function to aid with raising errors from a requests call
* __\_\_do\_rest\_api\_request(self, method, action, data=None, params=None, extra_headers=None,             api_url_template=None)__ - helper function to handle REST API requests.
* __\_\_read_experiment_JSON(self, expt_json)__ -
* __\_\_build_parameter_uri_lookup(self, schema_namespace)__ -
* __\_\_is_experiment_in_myTardis(self, title, res_id, schema_namespace)__ -
* __\_\_username_uri_lookup(self, upi)__ -
* __\_\_do_create_experiment(self, expt_json)__ -
* __\_\_do_create_dataset(self, dataset_json)__ -
* __\_\_get_instrument_by_name(self, name, facility=None)__ -
* __\_\_md5_python(self, file_path, blocksize=None)__ -
* __\_\_md5_subprocess(self, file_path, md5sum_executable='/usr/bin/md5sum')__ -
* __\_\_md5_file_calc(self, file_path, blocksize=None, subprocess_size_threshold=10\*1024\*1024, md5sum_executable='/usr/bin/md5sum')__ -
* __\_\_get_experiment_uri(self, Expt_title, Expt_handle)__ -
* __\_\_get_dataset(self, Expt_title, Expt_handle, Dataset_description)__ -
* __\_\_push_datafile(self, file_name, file_path, dataset_uri, parameter_sets_list=None, md5_checksum=None)__ -

### Public functions:
* __dict_to_json(d)__ - read a Python dictionary, d, serialise and return a JSON
* __do_post_request(self, action, data, extra_headers=None)__ - __POST__ (create) an item in the myTardis. Takes __action__ and __data__ as parameters, where __action__ is the myTardis structure (experiment, dataset, datafile etc) and __data__ is a JSON containing the information and metadata that fills the __POST__ request. Wraps around the __\_\_do_rest_api_request__ function.
* __do_get_request(self, action, params, extra_headers=None)__ - __GET__ (recall) an item in myTardis. Takes __action__ and __params__ as parameters, where __action__ is the myTardis structure and __params__ are the search parameters to refine the return from the __GET__ request. Wraps around the __\_\_do_rest_api_request__ function.
* __create_experiment_in_myTardis(self, json_file)__ -
* __create_dataset(self, dataset_json)__ -
* __delete_epxt_by_id(self, idnum)__ -
* __create_datafile(self, datafile_json)__ -

**Updated Text**

# Config for ingestion

Two sets of environment files are used by the ingestor. 

*Global_env* is a list of global settings that are used across multiple classes and include:

* PROXY_HTTP: http and proxy url where needed
* PROXY_HTTPS: https proxy url where needed
* LDAP_URL: URL for the LDAP service
* LDAP_USER_ATTR_MAP: dictionary linking different user attributes to a common framework for future compatibility
* LDAP_ADMIN_USER: the user name for the admin user in LDAP
* LDAP_ADMIN_PASSWORD: the admin user's password for LDAP
* LDAP_USER_BASE: String defining the LDAP structure for search
* PROJECT_DB: TODO - this is not yet defined
* RAID_API: TODO - this is not yet defined

*Local_env* contains specific configuration for the facility in question and includes:

* MYTARDIS_URL: URL to the specific instance of MyTardis to ingest into
* MYTARDIS_INGEST_USER: the service account the handles the ingestion
* MYTARDIS_INGEST_API_KEY: the api key associated with this service account
* MYTARDIS_FACILITY_MANAGER: a default user to associate the experiment with - a backstop to allow facility managers to assign experiments to users
* MYTARDIS_VERIFY_CERT: a boolean that enforces SSL certificate verification when ingesting
* MYTARDIS_EXPERIMENT_SCHEMA: the experiment schema to use with this ingestion - Note the schema configs should contain a dictionary with at least one 'DEFAULT' key linking to the default schema for this object. Additional keys are available to allow for multiple schema in a given object. For example, one for a raw data dataset and another for a processed data dataset.
* MYTARDIS_DATASET_SCHEMA: the dataset schema to use with this ingestion
* MYTARDIS_DATAFILE_SCHEMA: the datafile schema to use with this ingestion
* MYTARDIS_STORAGE_BOX: the name of the storage box in MyTardis in which the datafiles are being stored
* FILEHANDLER_S3_BUCKET: the S3 bucket to create objects in when moving from 'local' storage
* FILEHANDLER_REMOTE_ROOT: the root directory which the relative file paths relate to in the object store

# Dictionary formats for ingestion into MyTardis using the MyTardisUploader class

## Experiment

The bare minimum required to create an experiment as at 14/02/2020 is as follows:

| Key | Value|
|---|---|
|title: | The name of the experiment as displayed in MyTardis|
|description: | A short description of the sample|
|internal_id: | A unique identifier to the experiment; preferably a RaID or similar PID|
|project_id: | A unique identifier to the project that the experiment is part of; preferably a RaID or similar PID|
|institution_name: | Included for completeness; defaults to University of Auckland|
|created_time: | A datetime string included for completeness; defaults to Now at the time of ingestion|
|owners: | A Python list of owners (UPIs) who have access to the experiment, defaults to Facility Manager (defined in config)|
|users: | A Python list of users (UPIs) who have access to the experiment, defaults to None|
|groups: | A Python list of groups that have access to the experiment, defaults to None|

Any additional keys are added to a parameter set that is built with the experiment.

## Dataset

The bare minimum required to create an dataset as at 14/02/2020 is as follows:

|Key |Value |
|---|---|
|description: | The name of the dataset|
|dataset_id: | A unique identifier for the dataset; preferably a RaID or similar PID|
|internal_id: | The unique identifier to an experiment that exists. If the internal_id cannot be found then creation will stop| 

If *instrument* and/or *facility* keys are found, these will be used to link the dataset to the appropriate instrument.

Any additional keys are added to a parameter set that is built with the dataset.

## Datafile

The bare minimum required to create an datafile as at 14/02/2020 is as follows:

|Key |Value |
|---|---|
|file_name: | The name of the data file being uploaded|
|dataset_id: | The unique identifier to a dataset that exists. If the dataset_id cannot be found then creation will stop|
|remote_path: | The path to the directory that the file will be stored in once transfered to the data centre. *Note:* this is the final storage location|
|local_path: | The path to the local directory that the file was taken from. Used to uniquely identify file so that the checksum_digest can be used|
|size: | The file size in bytes|
|md5sum: | A suitable checksum for verification purposes|

Any additional keys are added to a parameter set that is built with the datafile
