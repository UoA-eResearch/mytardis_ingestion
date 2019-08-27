# myTardis Ingestion
Ingestion scripts to automate creation and data incorporation for UoA iteration of myTardis

# uploader.py
Uploader is the main ingestion script that interacts with myTardis.

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

## JSON files:

### Config:

The config file is a JSON containing the foloowing fields:

    "username": UoA UPI,
    "server": myTardis server IP aaddress,
    "api_key": API key,
    "root_dir": The top level directory of the staging space
        
__Note:__ API keys are user specific and defined through the myTardis admin interface. For most purposes, these will be kept solely for the harvester to interact with myTardis.

### Experiment JSON:

The form of the experiment JSON is, like that of the dataset and datafile, dependant on the schema that is being implemented. There are, however, 3 fields that must be present, _schema\_namespace_, _title_, and _handle_:

    "schema_namespace": URL to schema used in creating the experiment,
    Internal ID (can be anything as it is not used): {
        	"title": A string naming the experiment
	    	"handle": A unique identifier, can be a RaID or similar, or an internal standard
        	}
        
Optional keys include:

    Internal ID: {
        	"decription": Text that describes the experiment,
        	Metadata_parameter_key: Value to store
		}
       
While ideally the metadata parameters should be defined in the _schema\_namesapce_, this is not strictly necessary. Any additional parameters used will be added to the shcema.

Multiple experiments using the same schema can be defined in the JSON by specifying multiple Internal IDs. It should also be noted that the _"description"_ and _Metadata key_ key/value pairs need to be included within the individual Iternal IDs, at the same level as the _'title"_ and _"handle"_ fields.
