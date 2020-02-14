# UoA MyTardis Ingestor
# Written by Chris Seal <c.seal@auckland.ac.nz>
# Adapted from original ngs_ingestor script
# Thanks Andrew Perry <Andrew.Perry@monash.edu>
# Steve Androulakis <steve.androulakis@monash.edu>
# Grischa Meyer <grischa.meyer@monash.edu> for initial scripts

import logging
from requests.auth import AuthBase
from ..helper import check_dictionary, dict_to_json, get_user_from_upi
import backoff
import requests
import os
from urllib.parse import urljoin, urlparse
import json
import ldap3
from decouple import Config, RepositoryEnv
from pathlib import Path

logger = logging.getLogger(__name__)

class TastyPieAuth(AuthBase):
    """
    Attaches HTTP headers for Tastypie API key Authentication to the given
    Request object.
    #
    Because this ingestion script will sit inside the private network and
    will act as the primary source for uploading to myTardis, authentication
    via a username and api key is used.
    """
    def __init__(self, username, api_key):
        self.username = username
        self.api_key = api_key
    def __call__(self, r):
        r.headers['Authorization'] = 'ApiKey %s:%s' % (self.username,
                                                       self.api_key)
        return r

class MyTardisUploader:
    user_agent_name = __name__
    user_agent_url = 'https://github.com/UoA-eResearch/mytardis_ingestion.git'

    def __init__(self,
                 global_config_file_path,
                 local_config_file_path):
        '''Initialise uploader.

        Inputs:
        =================================
        config_file_path: A Path object pointing to an environment file that defines the ingestor behvaiour.'''
        '''
        Details of config_dict:
        =================================
        The config_dict must contain the following key/value pairs:
        
        Key / Value:
        =================================
        server / URL to the mytardis server. Default for auckland is https://mytardis.nectar.auckland.ac.nz
        root_dir / Top level directory for file paths
        username / The harvester username in MyTardis
        api_key  / API key for MyTardis access
        
        Returns:
        =================================
        Nil

        Members:
        =================================
        self.server: URL to the MyTardis server
        self.api_url: URL to the REST API for the MyTardis server
        self.root_dir: Top level directory for file paths
        self.cwd: The current working directory (where the script is being run from typically)
        self.auth: TastyPie authorisation for the username/API combination
        self.user_agent: User agent for the REST API calls
        self.verify_certificate: Flag telling MyTardis to verify the security certificate
        
        required_keys = ['server',
                         'username',
                         'api_key',
                         'storage_box']
        check = check_dictionary(config_dict,
                                 required_keys)
        if not check[0]:
            logger.error(f'Config dictionary is incomplete. Missing keys: {", ".join(check[1])}')
            raise Exception('Initialisation of the ingestor failed')
        else:
            self.server = config_dict['server']
            self.api_url = urljoin(config_dict['server'],
                                   '/api/v1/%s/')
            self.harvester = harvester
            self.cwd = os.getcwd()
            
            
            # Not sure that we want to force this but lets see if it breaks
            self.verify_certificate = False # changed to avoid SSLError - need to revist
            self.storage_box = config_dict['storage_box']
        '''
        # global_config holds environment variables that don't change often such as LDAP parameters and project_db stuff
        global_config = Config(RepositoryEnv(global_config_file_path))
        # local_config holds the details about how this particular set of data should be handled
        local_config = Config(RepositoryEnv(local_config_file_path))
        self.server = local_config('MYTARDIS_URL')
        self.ingest_user = local_config('MYTARDIS_INGEST_USER')
        self.ingest_api_key = local_config('MYTARDIS_INGEST_API_KEY')
        self.verify_certificate = local_config('MYTARDIS_VERIFY_CERT',
                                               default=True,
                                               cast=bool)
        self.experiment_schema = local_config('MYTARDIS_EXPT_SCHEMA')
        self.dataset_schema = local_config('MYTARDIS_DATASET_SCHEMA')
        self.datafile_schema = local_config('MYTARDIS_DATAFILE_SCHEMA')
        self.proxies = {"http": global_config('PROXY_HTTP',
                                             default=None),
                        "https": global_config('PROXY_HTTPS',
                                             default=None)}
        if self.proxies['http'] == None and self.proxies['https'] == None:
            self.proxies = None
        self.ldap_dict = {}
        self.ldap_dict['url'] = global_config('LDAP_URL')
        self.ldap_dict['user_attr_map'] = global_config('LDAP_USER_ATTR_MAP')
        self.ldap_dict['admin_user'] = global_config('LDAP_ADMIN_USER')
        self.ldap_dict['admin_password'] = global_config('LDAP_ADMIN_PASSWORD')
        self.ldap_dict['user_base'] = global_config('LDAP_USER_BASE')
        self.api_url = urljoin(self.server,
                               '/api/v1/%s/')
        self.user_agent = '%s/%s (%s)' % (MyTardisUploader.user_agent_name,
                                          '1.1',
                                          MyTardisUploader.user_agent_url)
        self.auth = TastyPieAuth(self.ingest_user,
                                 self.ingest_api_key)

    def __resource_uri_to_id(self, uri):
        """
        Takes resource URI like: http://example.org/api/v1/experiment/998
        and returns just the id value (998).
        #
        :type uri: str
        :rtype: int"""
        resource_id = int(urlparse(uri).path.rstrip(
            os.sep).split(os.sep).pop())
        return resource_id
    
    # =================================
    #
    # Functions to access the API
    #
    # __rest_api_call
    # __get_request
    # __post_request
    #
    # =================================

    def __raise_request_exception(self, response):
        e = requests.exceptions.RequestException(response=response)
        e.message = "%s %s" % (response.status_code, response.reason)
        raise e
    
    @backoff.on_exception(backoff.expo,
                          requests.exceptions.RequestException,
                          max_tries=8)
    def __rest_api_request(self,
                           method,  # REST api method
                           action,  # action here refers to experiment, dataset or datafile
                           data=None,
                           params=None,
                           extra_headers=None,
                           api_url_template=None):
        '''Function to handle the REST API calls
        
        Inputs:
        =================================
        method: The REST API method, POST, GET etc.
        action: The object type to call REST API on, e.g. experiment, dataset
        data: A JSON string containing data for generating an object via POST/PUT
        params: A JSON string of parameters to be passed in the URL
        extra_headers: Extra headers (META) to be passed to the API call
        api_url_template: Over-ride for the default API URL
        
        Returns:
        =================================
        A Python Requests library repsonse object
        '''
        if api_url_template is None:
            api_url_template = self.api_url
        url = api_url_template % action
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/json',
                   'User-Agent': self.user_agent}
        if extra_headers:
            headers = {**headers, **extra_headers}
        logger.debug(url)
        try:
            if self.proxies:
                response = requests.request(method,
                                            url,
                                            data=data,
                                            params=params,
                                            headers=headers,
                                            auth=self.auth,
                                            verify=self.verify_certificate,
                                            proxies=self.proxies)
            else:
                response = requests.request(method,
                                            url,
                                            data=data,
                                            params=params,
                                            headers=headers,
                                            auth=self.auth,
                                            verify=self.verify_certificate)
            # 502 Bad Gateway triggers retries, since the proxy web
            # server (eg Nginx or Apache) in front of MyTardis could be
            # temporarily restarting
            if response.status_code == 502:
                self.__raise_request_exception(response)
            else:
                response.raise_for_status()
        except requests.exceptions.RequestException as err:
            logger.error("Request failed : %s : %s", err.message, url)
            raise err
        except Exception as err:
            logger.error(f'Error, {err.message}, occurred when attempting to call api request {url}')
            raise err
        return response
    
        def __get_request(self,
                        action,
                        params,
                        extra_headers=None):
        '''Wrapper around self._do_rest_api_request to handle GET requests

        Inputs:
        =================================
        action: the type of object, (e.g. experiment, dataset) to GET
        params: parameters to pass to filter the request return
        extra_headers: any additional information needed in the header (META) for the object being created
        
        Returns:
        =================================
        A Python requests module response object'''
        try:
            response = self.__rest_api_request('GET',
                                                  action,
                                                  params=params,
                                                  extra_headers=extra_headers)
        except Exception as err:
            raise err
        return response

    def __post_request(self,
                         action,
                         data,
                         extra_headers=None):
        '''Wrapper around self._do_rest_api_request to handle POST requests

        Inputs:
        =================================
        action: the type of object, (e.g. experiment, dataset) to POST
        data: a JSON string holding the data to generate the object
        extra_headers: any additional information needed in the header (META) for the object being created
        
        Returns:
        =================================
        A Python requests module response object'''
        try:
            response = self.__rest_api_request('POST',
                                                  action,
                                                  data=data,
                                                  extra_headers=extra_headers)
        except Exception as err:
            raise err
        return response

    # =================================
    #
    # End of API call functions
    #
    # =================================


    # =================================
    #
    # Functions to get URIs out of MyTardis
    #
    # =================================

    def __get_uri(self,
                  action,
                  query_params):
        '''General solution for finding resource uri's from the 
        database.

        Inputs:
        =================================
        action: the object being retrieved
        query_params: search parameters

        Returns:
        =================================
        URI if one object with the search name exists'''
        try:
            response = self.__get_request(action,
                                          params = query_params)
        except Exception as error:
            logger.error(error.message)
            raise error
        else:
            response_dict = json.loads(response.text)
            logger.debug(response_dict)
            if response_dict == [] or response_dict['objects'] == []:
                return False
            elif len(response_dict['objects']) > 1:
                logger.error(
                    f'Multiple instances of {action} {query_params} found in the database. Please verify and clean up.')
                raise Exception(f'Multiple instances of {action} {query_params} found in the database. Please verify and clean up.')
            else:
                obj = response_dict['objects'][0]
                logger.debug(f'{action}: {obj} found in the database.')
                return obj['resource_uri']

    def __get_dataset_uri(self, dataset_id):
        '''Uses REST API GET with an dataset_id filter. Raises an error if multiple
        instances of the same dataset_id are located as this should never happen given
        the database restrictions.

        Inputs:
        =================================
        dataset_id: unique identifier for dataset

        Returns:
        =================================
        False if the id is not found in the database
        URI of the dataset if a single instance of the id is found in the database
        '''
        query_params = {u'dataset_id': dataset_id}
        try:
            response = self.__get_uri('dataset', query_params)
        except Exception as error:
            raise
        return response

    def __get_experiment_uri(self, internal_id):
        '''Uses REST API GET with an internal_id filter. Raises an error if multiple
        instances of the same internal_id are located as this should never happen given
        the database restrictions.

        Inputs:
        =================================
        internal_id: unique identifier for experiment

        Returns:
        =================================
        False if the id is not found in the database
        URI of the experiment if a single instance of the id is found in the database
        '''
        query_params = {u'internal_id': internal_id}
        try:
            response = self.__get_uri('experiment', query_params)
        except Exception as error:
            raise
        return response

    def __get_group_uri(self, group):
        '''Use the user api in myTardis to search on groups

        Inputs:
        =================================
        group: The group name

        Returns:
        =================================
        False if group is not in the database
        -1 if more than one group with the same name exists. 
        URI to the goup id if exactly one group with the name is found.
        '''
        query_params = {u'name': group}
        try:
            response = self.__get_uri('group', query_params)
        except Exception as error:
            raise
        return response
    
    def __get_instrument_uri(self, name, facility=None):
        '''Read database for instruments and return resource_uri for an
        instrument by name and facility
        
        Inputs:
        =================================
        name: Instrument name
        facility: The host facility for the instrument, defaults to None

        Returns:
        =================================
        False if the instrument is not present or if it cannot be uniquely identified
        URI of the instrument if it can be uniquely identified
        '''
        query_params = {u'name': name}
        if facility is not None:
            query_params['facility__name'] = facility
        try:
            response = self.__get_uri('instrument', query_params)
        except Exception as error:
            raise
        return response

    def __get_schema_uri(self,
                         namespace):
        '''Reads the database for schema and returns a resource_uri for one by name
        Raises an execption if no schema can be found, or if there are multiple with the
        same namespace

        Inputs:
        =================================
        namespace: the schema namespace

        Returns:
        =================================
        URI if one schema with the search namespace.
        '''
        query_params = {'namespace': namespace}
        try:
            response = self.__get_uri('schema', query_params)
        except Exception as error:
            raise
        return response

    def __get_user_uri(self, username):
        '''Use the user api in myTardis to search on the username (should be UoA UPI).
        
        Inputs:
        =================================
        username: The user name to look up. Should be a UoA UPI

        Returns:
        =================================
        False if user name is not in the database: ToDo: If this is the case look up the UoA LDAP
            and add a user if they exist within the university.
        -1 if more than one user with the same name exists.
        URI to the user if exactly one group with the name is found.
        '''
        query_params = {'username': username}
        try:
            response = self.__get_request('user',
                                           params=query_params)
        except Exception as error:
            logger.error(error.message)
            raise error
        else:
            resp_dict = json.loads(response.text)
            if resp_dict['objects'] == []:
                logger.info(f'UPI: {username} is not in the user database.')
                return False
            elif len(resp_dict['objects']) > 1:
                logger.error(f'Multiple instances of UPI: {upi} found in user database. Please verify and clean up.')
                raise Exception(f'Multiple instances of UPI: {upi} found in user database. Please verify and clean up.')
            else:
                obj = resp_dict['objects'][0]
                logger.debug(obj)
                return obj['resource_uri']

    def __get_user_profile(self,
                           user_id):
        '''

        '''
        query_params = {u'user': user_id}
        try:
            response = self._do_get_request('userprofile',
                                            query_params)
            response.raise_for_status()
        except Exception as error:
            logger.error(error.message)
            raise error
        resp_dict = json.loads(response.text)
        if resp_dict['objects'] == []:
            return False
        elif len(resp_dict['objects']) > 1:
            logger.error(f'More than one user profile found for user {username}')
            raise Exception(f'More than one user profile found for user {username}')
        else:
            obj = resp_dict['objects'][0]
        user_uri = obj['resource_uri']
        return user_uri

    # =================================
    #
    # End of MyTardis Get functions
    #
    # =================================

    # =================================
    #
    # MyTardis User functions - create and assign access
    #
    # =================================
    
    def __get_or_create_user(self,
                            upi):
        uri = self.__get_user_uri(upi)
        if uri:
            return (False, uri)
        else:
            try:
                person = get_user_from_upi(self.ldap_dict,
                                           upi)
            except Exception as error:
                raise
            if not person:
                return (False, None)
            person_json = dict_to_json(person)
            try:
                response = self.__do_post_request('user',
                                                  person_json)
                response.raise_for_status()
            except Exception as err:
                logger.error(f'Error occurred when creating user {username}. Error: {err}')
                return (False, None)
            response = json.loads(response.text)
            uri = response['resource_uri']
            user_id = self.__resource_uri_to_id(uri)
            user_profile_uri = self.__get_user_profile(user_id)
            user_profile = {'resource_uri': user_profile_uri,
                            'user': uri}
            mytardis = {'username': username,
                        'user_id' : user_id,
                        'userProfile': user_profile}
            mytardis_json = dict_to_json(mytardis)
            try:
                response = self.__do_post_request('userauthentication',
                                                  mytardis_json)
                response.raise_for_status()
            except Exception as error:
                logger.error(error.message)
                return (False, None)
        return (True, uri)

    def __get_ownership_int(self, ownership_type):
        ownership_type_mappings = {
            u'Owner-owned': 1,
            u'System-owned': 2,
        }
        v = ownership_type_mappings.get(ownership_type, None)
        if v is None:
            raise ValueError("Valid values of acl_ownership_type are %s" %
                             ' or '.join(ownership_type_mappings.keys()))
        return v

    def share_experiment_with_group(self,
                                    experiment_uri,
                                    group_uri,
                                    *args,
                                    **kwargs):
        """
        Executes an HTTP request to share an experiment with a group,
        via updating the ObjectACL.

        Inputs:
        =================================
        experiment_uri: The integer ID or URL path to the Experiment.
        group_uri: The integer ID or URL of the group we with share with.
        
        Returns:
        =================================
        A requests Response object
        """
        return self.__share_experiment(experiment_uri,
                                       'django_group',
                                       group_uri,
                                       isOwner=False,
                                       *args,
                                       **kwargs)

    def share_experiment_with_user(self,
                                   experiment_uri,
                                   user_uri,
                                   *args,
                                   **kwargs):
        """
        Executes an HTTP request to share an experiment with a user,
        via updating the ObjectACL.

        Inputs:
        =================================
        experiment_uri: The integer ID or URL path to the Experiment
        user_uri: The integer ID or URL of the User to share with.
        
        Returns:
        =================================
        A requests Response object
        """
        return self.__share_experiment(experiment_uri,
                                       'django_user',
                                       user_uri,
                                       isOwner=False,
                                       *args,
                                       **kwargs)

    def share_experiment_with_owner(self,
                                    experiment_uri,
                                    user_uri,
                                    *args,
                                    **kwargs):
        """
        Executes an HTTP request to share an experiment with the project owner,
        via updating the ObjectACL.

        Inputs:
        =================================
        experiment_uri: The integer ID or URL path to the Experiment
        user_uri: The integer ID or URL of the User to share with.
        
        Returns:
        =================================
        A requests Response object
        """
        return self.__share_experiment(experiment_uri,
                                       'django_user',
                                       user_uri,
                                       isOwner=True,
                                       *args,
                                       **kwargs)

    def __share_experiment(self,
                           content_object,
                           plugin_id,
                           entity_object,
                           isOwner=False,
                           content_type=u'experiment',
                           acl_ownership_type=u'Owner-owned'):
        """
        Executes an HTTP request to share an MyTardis object with a user or
        group, via updating the ObjectACL.
        #
        :param content_object: The integer ID or URL path to the Experiment,
                               Dataset or DataFile to update.
        :param plugin_id: django_user or django_group
        :param content_type: Django ContentType for the target object, usually
                             'experiment', 'dataset' or 'datafile'
        :type content_object: union(str, int)
        :type plugin_id: str
        :type content_type: basestring
        :return: A requests Response object
        :rtype: Response
        """
        # TODO Refactor to remove six
        import six
        if isinstance(content_object, six.string_types):
            object_id = self._resource_uri_to_id(content_object)
        elif isinstance(content_object, int):
            object_id = content_object
        else:
            raise TypeError("'content_object' must be a URL string or int ID")
        if isinstance(entity_object, six.string_types):
            entity_id = self._resource_uri_to_id(entity_object)
        elif isinstance(entity_object, int):
            entity_id = enitity_object
        else:
            raise TypeError("'entity_object' must be a URL string or int ID")
        acl_ownership_type = self._get_ownership_int(acl_ownership_type)
        data = {
            u'pluginId': plugin_id,
            u'entityId': str(entity_id),
            u'content_type': str(content_type),
            u'object_id': str(object_id),
            u'aclOwnershipType': acl_ownership_type,
            u'isOwner': isOwner,
            u'canRead': True,
            u'canWrite': isOwner,
            u'canDelete': False,
            u'effectiveDate': None,
            u'expiryDate': None
        }
        response = self.__do_post_request('objectacl',
                                          dict_to_json(data))
        return response
    
    # =================================
    #
    # End of MyTardis User functions
    #
    # =================================

    def _build_datafile_dictionaries(self,
                                     datafile_dict,
                                     required_keys):
        '''Read in a datafile dictionary and build the file dictionary needed to create
        the datafile in mytardis
        
        Inputs:
        =================================
        datafile_dict: A dictionary containing the definintion of the dataset and its metadata
        
        The dataset_dict must contain the following key/value pairs
        
        Key / Value:
        =================================
        schema_namespace / the schema defining the dataset metadata
        dataset_id / An internal unique identifer for the dataset
        file_name / The file name
        remote_path / The relative path to the storage in the remote directory
        mimetype / The MIME type of the file
        size / The file size
        md5sum / the md5 check sum for the file

        Returns:
        =================================
        mytardis: A dictionary containing the details necessary to create the datafile in myTardis
        '''
        from datetime import datetime
        mytardis = {}
        params = {}
        paramset = {}
        try:
            paramset['schema'] = self._get_schema_uri(datafile_dict.pop('schema_namespace'))
        except Exception as err:
            raise err
        try:
            uri = self._get_dataset_uri(datafile_dict.pop('dataset_id'))
        except Exception as err:
            raise
        else:
            if not uri:
                logger.warning(f'Dataset ID {datafile_dict["dataset_id"]} not found in the database, skipping.')
                raise Exception(f'Dataset ID {datafile_dict["dataset_id"]} not found in the database, skipping.')
            else:
                mytardis['dataset'] = uri
        filename = datafile_dict.pop('file')
        mytardis['filename'] = filename
        remote_path = datafile_dict.pop('remote_dir')
        mytardis['directory'] = os.path.join('Solarix', remote_path)
        for key in datafile_dict:
            if key in required_keys:
                mytardis[key] = datafile_dict[key]
            elif key == 'local_dir':
                continue
            else:
                params[key] = datafile_dict[key]            
        store_loc = {u'uri': os.path.join(self.harvester.filehandler.s3_root_dir, remote_path, filename),
                     u'location': self.storage_box,
                     u'protocol': u'file'}
        mytardis['replicas'] = [store_loc]
        parameter_list = []
        for key in params.keys():
            parameter_list.append({u'name': key,
                                   u'value': params[key]})
        if parameter_list != []:
            paramset['parameters'] = parameter_list
            mytardis['parameter_sets'] = paramset
        return mytardis


    def _build_dataset_dictionaries(self,
                                     dataset_dict,
                                     required_keys):
        '''Read in a dataset dictionary and build the mytardis and params dictionary needed to create
        the dataset in mytardis
        
        Inputs:
        =================================
        dataset_dict: A dictionary containing the definintion of the dataset and its metadata
        
        The dataset_dict must contain the following key/value pairs
        
        Key / Value:
        =================================
        internal_id / An internal unique identifier for the experiment the dataset is associated with
        schema_namespace / the schema defining the dataset metadata
        description / The name of the dataset. This should be unique
        dataset_id / An internal unique identifer for the dataset

        Returns:
        =================================
        mytardis: A dictionary containing the details necessary to create a dataset in myTardis
        paramset: A dictionary containing metadata to be attached to the dataset
        '''
        from datetime import datetime
        mytardis = {}
        params = {}
        paramset = {}
        defaults = {'instrument': None,
                    'created_time': datetime.utcnow()}
        for key in defaults.keys():
            if key in dataset_dict.keys():
                mytardis[key] = dataset_dict[key]
            else:
                mytardis[key] = defaults[key]
        for key in dataset_dict.keys():
            if key == 'schema_namespace': # This is a special case where the URI is needed
                try:
                    paramset['schema'] = self._get_schema_uri(dataset_dict[key])
                except Exception as err:
                    raise err
            elif key == 'internal_id':
                try:
                    uri = self._get_experiment_uri(dataset_dict['internal_id'])
                except Exception as err:
                    raise
                else:
                    if not uri:
                        logger.error(f'Experiment ID {dataset_dict["internal_id"]} not found in the database, skipping.')
                        raise Exception(f'Experiment ID {dataset_dict["internal_id"]} not found in the database, skipping.')
                    else:
                        mytardis['experiments'] = [uri]
            else:
                if key in required_keys and key != 'schema_namespace':
                    mytardis[key] = dataset_dict[key]
                elif key in defaults.keys():
                    continue
                else:
                    params[key] = dataset_dict[key]
        if 'instrument' in params.keys():
            instrument = params['instrument']
            if 'facility' in params[key]:
                facility = params['facility']
            else:
                facility = None
            instrument_uri = self._get_instrument_uri(instrument,
                                                       facility)
            if instrument_uri:
                mytardis['instrument'] = instrument_uri
        parameter_list = []
        for key in params.keys():
            if key == 'instrument' or key == 'facility':
                continue
            parameter_list.append({u'name': key,
                                   u'value': params[key]})
        paramset['parameters'] = parameter_list
        return (mytardis, paramset)

    def _build_experiment_dictionaries(self,
                                       expt_dict,
                                       required_keys):
        '''Read in an experiment dictionary and build the mytardis and params dictionary needed to create
        the experiment in mytardis
        
        Inputs:
        =================================
        expt_dict: A dictionary containing the definintion of the experiment and its metadata
        
        The expt_dict must contain the following key/value pairs
        
        Key / Value:
        =================================
        title / Name of the experiment
        internal_id / An internal unique identifier for the experiment
        schema_namespace / the schema defining the experiment metadata
        project_id / Defaults to No Project
        institution_name / Defaults to University of Auckland
        created_time / Datetime string, defaults to Now
        description / Defaults to No description
        users / list of MyTardis users who should have access to the experiment, defaults to None
        groups / list of MyTardis groups who should have access to the experiment, defaults to None

        Returns:
        =================================
        mytardis: A dictionary containing the details necessary to create an experiment in myTardis
        paramset: A dictionary containing metadata to be attached to the experiment
        users: A dictionary containing the usernames of allowed users
        groups: A dictionary containing the group names of allowed groups
        
        TODO:
        =================================
        Once the ExperimentAuthor model has been exposed in myTardis, handle authors and return
        an author dictionary.
        '''
        from datetime import datetime
        mytardis = {}
        params = {}
        users = None
        groups = None
        owners = None
        paramset = {}
        defaults = {'project_id':'No Project ID',
                    'institution_name':'University of Auckland',
                    'description': 'No description',
                    'created_time': datetime.utcnow()}
        for key in defaults.keys():
            if key in expt_dict.keys():
                mytardis[key] = expt_dict[key]
            else:
                mytardis[key] = defaults[key]
        for key in expt_dict.keys():
            if key == 'schema_namespace': # This is a special case where the URI is needed
                try:
                    paramset['schema'] = self._get_schema_uri(expt_dict[key])
                except Exception as err:
                    raise err
            else:
                if key in required_keys:
                    mytardis[key] = expt_dict[key]
                elif key in defaults.keys() or key =='schema_namespace':
                    continue
                elif key == 'users':
                    users = expt_dict[key]
                elif key == 'groups':
                    groups = expt_dict[key]
                elif key == 'owners':
                    owners = expt_dict[key]
                else:
                    params[key] = expt_dict[key]
        parameter_list = []
        for key in params.keys():
            #for value in params[key]:
            parameter_list.append({u'name': key,
                                   u'value': params[key]})
        paramset['parameters'] = parameter_list
        return (mytardis, paramset, owners, users, groups)
            






    
    

    
    
    

    
    
    

    
                    
    

    def create_experiment(self,
                          expt_dict):
        '''Read in an experiment dictionary. Check against the myTardis database to see
        if an experiment with the same RAID exists. If it does, fail gracefully
        otherwise create and experiment using POST.
        
        Inputs:
        =================================
        expt_dict: A dictionary containing the definintion of the experiment and its metadata
        
        The expt_dict must contain the following key/value pairs
        
        Key / Value:
        =================================
        title / Name of the experiment
        internal_id / An internal unique identifier for the experiment
        schema_namespace / the schema defining the experiment metadata
        project_id / Defaults to No Project
        institution_name / Defaults to University of Auckland
        created_time / Datetime string, defaults to Now
        description / Defaults to No description
        users / list of MyTardis users who should have access to the experiment, defaults to None
        groups / list of MyTardis groups who should have access to the experiment, defaults to None
        
        Returns:
        =================================
        True and the URI if the experiment is created successfully
        False and the URI if the experiment already exists in the database as determined from internal_id
        False and None empty dictionary if creation fails.
        '''
        os.chdir(self.harvester.root_dir)
        required_keys = ['title',
                         'internal_id',
                         'schema_namespace']
        check = check_dictionary(expt_dict,
                                 required_keys)
        if not check[0]:
            logger.error(f'The experiment dictionary is incomplete. Missing keys: {", ".join(check[1])}')
            return (False, None)
        try:
            uri = self._get_experiment_uri(expt_dict['internal_id'])
        except Exception as err:
            logger.error(f'Encountered error: {err}, when looking for experiment')
            return (False, None)
        if uri:
            return (False, uri)
        else:
            try:
                mytardis, paramset, owners, users, groups = \
                    self._build_experiment_dictionaries(expt_dict,
                                                         required_keys)
            except Exception as err:
                logger.error(f'Encountered error: {err} when building experiment dictionaries')
                return (False, None)
            mytardis_json = dict_to_json(mytardis)
            try:
                response = self._do_post_request('experiment',
                                                mytardis_json)
                response.raise_for_status()
            except Exception as err:
                logger.error(f'Error occurred when creating experiment {mytardis["title"]}. Error: {err}')
                return (False, None)
            response = json.loads(response.text)
            uri = response['resource_uri']
            paramset['experiment'] = uri
            paramset_json = dict_to_json(paramset)
            try:
                response = self._do_post_request('experimentparameterset',
                                                paramset_json)
                response.raise_for_status()
            except Exception as err:
                logger.error(f'Error occurred when attaching metadata to experiment {mytardis["title"]}. Error: {err}')
            if groups:
                for group in groups:
                    try:
                        group_uri = self._get_group_uri(group)
                    except Exception as err:
                        logger.error(f'Error: {err} occured when searching for group {group}')
                    else:
                        try:
                            response = self.share_experiment_with_group(uri,
                                                                        group_uri)
                            response.raise_for_status()
                        except Exception as err:
                            logger.error(f'Error: {err} occured when allocating group {group} access to experiment: {mytardis["title"]}')
            if users:
                for user in users:
                    try:
                        flg, user_uri = self._get_or_create_user(user)
                    except Exception as err:
                        logger.error(f'Error: {err} occured when searching for user {user}')
                    else:
                        try:
                            response = self.share_experiment_with_user(uri,
                                                                       user_uri)
                            response.raise_for_status()
                        except Exception as err:
                            logger.error(f'Error: {err} occured when allocating user {user} access to experiment: {mytardis["title"]}')
            if owners:
                for user in owners:
                    try:
                        flg, user_uri = self._get_or_create_user(user)
                    except Exception as err:
                        logger.error(f'Error: {err} occured when searching for user {user}')
                    else:
                        try:
                            response = self.share_experiment_with_owner(uri,
                                                                        user_uri)
                            response.raise_for_status()
                        except Exception as err:
                            logger.error(f'Error: {err} occured when allocating default user {user} access to experiment: {mytardis["title"]}')
            os.chdir(self.cwd)
            return (True, uri)

    @backoff.on_exception(backoff.expo,
                          requests.exceptions.RequestException,
                          max_tries=8)
    def create_dataset(self, dataset_dict):
        '''Read in a dataset dictionary. Check against the myTardis database to see
        if an experiment with the same RAID exists. If it does, create the dataset using POST.
        
        Inputs:
        =================================
        dataset_dict: A dictionary containing the definintion of the dataset and its metadata
        
        The dataset_dict must contain the following key/value pairs
        
        Key / Value:
        =================================
        internal_id / An internal unique identifier for the experiment the dataset is associated with
        schema_namespace / the schema defining the dataset metadata
        description / The name of the dataset. This should be unique
        dataset_id / An internal unique identifer for the dataset

        Returns:
        =================================
        True and the URI if the dataset is created successfully
        False and the URI if the dataset already exists in the database as determined from dataset_id
        False and None if creation fails.
        '''
        os.chdir(self.harvester.root_dir)
        from datetime import datetime
        required_keys = ['internal_id',
                         'schema_namespace',
                         'description',
                         'dataset_id']
        check = check_dictionary(dataset_dict,
                                 required_keys)
        if not check[0]:
            logger.error(f'The dataset dictionary is incomplete. Missing keys: {", ".join(check[1])}')
            return (False, None)
        try:
            dataset_uri = self._get_dataset_uri(dataset_dict['dataset_id'])
        except Exception as err:
            logger.error(f'Encountered error: {err}, when looking for dataset: {dataset_dict["description"]}')
            return (False, None)
        if dataset_uri:
            return (False, dataset_uri)
        else:
            try:
                expt = dataset_dict['internal_id']
                expt_uri = self._get_experiment_uri(expt)
                logger.debug(expt_uri)
            except Exception as err:
                logger.error(f'Encountered error: {err} when looking for experiment: {expt}')
                return (False, None)
            if not expt_uri:
                logger.error(f'Unable to locate experiment: {expt} in database')
                return (False, None)
            else:
                try:
                    mytardis, paramset = self._build_dataset_dictionaries(dataset_dict,
                                                                           required_keys)
                except Exception as err:
                    logger.error(f'Encountered error: {err} when building dataset dictionaries')
                    return (False, None)
                mytardis_json = dict_to_json(mytardis)
                try:
                    response = self._do_post_request('dataset',
                                                    mytardis_json)
                    logger.debug(response.text)
                    response.raise_for_status()
                except Exception as err:
                    logger.error(f'Error: {err} when creating dataset {mytardis["description"]}')
                    return (False, None)
                response = json.loads(response.text)
                uri = response['resource_uri']
                logger.debug(uri)
                paramset['dataset'] = uri
                paramset_json = dict_to_json(paramset)
                logger.debug(paramset_json)
                try:
                    response = self._do_post_request('datasetparameterset',
                                                    paramset_json)
                    logger.debug(response.text)
                    response.raise_for_status()
                except Exception as err:
                    logger.error(f'Error occurred when attaching metadata to experiment {mytardis["title"]}. Error: {err}')
        os.chdir(self.cwd)
        return (True, uri)

    
    
    def create_datafile(self, datafile_dict):
        '''Read in a datafile dictionary. If the file has already been pushed to
        s3 or Ceph storage, create a datafile object pointing to the stored data. If not
        upload the file through myTardis.

        Inputs:
        =================================
        datafile_dict: a dictionary containing the definition of the datafile and its metadata

        The datafile_dict must contain the following key/value pairs
        
        Key / Value:
        =================================
        schema_namespace / the schema defining the dataset metadata
        dataset_id / An internal unique identifer for the dataset
        file_name / The file name
        remote_path / The relative path to the storage in the remote directory
        mimetype / The MIME type of the file
        size / The file size
        md5sum / the md5 check sum for the filefile_name / The file name for the object to be stored

        Returns:
        =================================
        True and the URI if the datafile is created successfully
        False and None if creation fails.
        '''
        os.chdir(self.harvester.root_dir)
        required_keys = ['schema_namespace',
                         'dataset_id',
                         'file',
                         'remote_dir',
                         'mimetype',
                         'size',
                         'md5sum']
        check = check_dictionary(datafile_dict,
                                 required_keys)
        if not check[0]:
            logger.error(f'The datafile dictionary is incomplete. Missing keys: {", ".join(check[1])}')
            return (False, None)
        try:
            mytardis = self._build_datafile_dictionaries(datafile_dict,
                                                          required_keys)
        except Exception as err:
            logger.error(f'Encountered error: {err} when building datafile dictionaries')
            return (False, None)
        mytardis_json = dict_to_json(mytardis)
        try:
            response = self._do_post_request('dataset_file',
                                            mytardis_json)
            response.raise_for_status()
        except Exception as err:
            logger.error(f'Error: {err} eccountered when creating dataset_file {mytardis["filename"]}')
            return (False, None)
        return (True, None)
