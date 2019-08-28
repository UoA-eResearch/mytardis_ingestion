# UoA MyTardis Ingestor
# Written by Chris Seal <c.seal@auckland.ac.nz>
# Adapted from original ngs_ingestor script
# Thanks Andrew Perry <Andrew.Perry@monash.edu>
# Steve Androulakis <steve.androulakis@monash.edu>
# Grischa Meyer <grischa.meyer@monash.edu> for initial scripts

import logging
from requests.auth import AuthBase
from ..helper import check_dictionary, dict_to_json
import backoff
import requests
import os

logger = logging.getLogger(__name__)

class MyTardisUploader:
    user_agent_name = __name__
    user_agent_url = 'https://github.com/UoA-eResearch/mytardis_ingestion.git'

    def __init__(self, config_dict):
        '''Initialise uploader.

        Inputs:
        =================================
        config_dict: A dictionary containing the configuration details for the uploader
        
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
        '''
        required_keys = ['server',
                         'username',
                         'api_key',
                         'root_dir']
        check = check_dictionary(config_dict,
                                 required_keys)
        if not check[0]:
            logger.error(f'Config dictionary is incomplete. Missing keys: {", ".join(check[1])}')
            raise Exception('Initialisation of the ingestor failed')
        else:
            if 'proxies' in config_dict.keys():
                self.proxies = config_dict['proxies']
            else:
                self.proxies = None
            self.server = config_dict['server']
            self.api_url = urljoin(config_dict['server'],
                                   '/api/v1/%s/')
            self.root_dir = config_dict['root_dir']
            self.cwd = os.getcwd()
            self.auth = TastyPieAuth(config_dict['username'],
                                     config_dict['api_key'])
            self.user_agent = '%s/%s (%s)' % (MyTardisUploader.user_agent_name,
                                              '1.0',
                                              MyTardisUploader.user_agent_url)
            # Not sure that we want to force this but lets see if it breaks
            self.verify_certificate = True

    def __raise_request_exception(self, response):
        e = requests.exceptions.RequestException(response=response)
        e.message = "%s %s" % (response.status_code, response.reason)
        raise e

    @backoff.on_exception(backoff.expo,
                          requests.exceptions.RequestException,
                          max_tries=8)
    def __do_rest_api_request(self,
                              method,  # REST api method
                              action,  # action here refers to experiment, dataset or datafile
                              data=None,
                              params=None,
                              extra_headers=None,
                              api_url_template=None,
                              proxies=None):
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
            if proxies:
                response = requests.request(method,
                                            url,
                                            data=data,
                                            params=params,
                                            headers=headers,
                                            auth=self.auth,
                                            verify=self.verify_certificate,
                                            proxies=proxies)
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
            
    def __do_post_request(self, action, data, extra_headers=None):
        '''Wrapper around self.__do_rest_api_request to handle POST requests

        Inputs:
        =================================
        action: the type of object, (e.g. experiment, dataset) to POST
        data: a JSON string holding the data to generate the object
        extra_headers: any additional information needed in the header (META) for the object being created
        
        Returns:
        =================================
        A Python requests module response object'''
        try:
            response = self.__do_rest_api_request('POST',
                                                  action,
                                                  data=data,
                                                  extra_headers=extra_headers)
        except Exception as err:
            raise err
        return response
            

    def __do_get_request(self, action, params, extra_headers=None):
        '''Wrapper around self.__do_rest_api_request to handle GET requests

        Inputs:
        =================================
        action: the type of object, (e.g. experiment, dataset) to GET
        params: parameters to pass to filter the request return
        extra_headers: any additional information needed in the header (META) for the object being created
        
        Returns:
        =================================
        A Python requests module response object'''
        try:
            response = self.__do_rest_api_request('GET',
                                                  action,
                                                  params=params,
                                                  extra_headers=extra_headers)
        except Exception as err:
            raise err
        return response

    def __build_experiment_dictionaries(self,
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
        proxies / http and https proxies if needed to pass through the firewall

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
        paramset = {}
        defaults = {'project_id':'No Project',
                    'institution_name':'University of Auckland',
                    'description': 'No description',
                    'created_time': datetime.utcnow()}
        for key in defaults.keys():
            if key in expt_dict.keys():
                mytardis[key] = expt_dict.pop(key)
            else:
                mytardis[key] = defaults[key]
        for key in expt_dict.keys():
            if key == 'schema_namespace': # This is a special case where the URI is needed
                schema = expt_dict.pop(key)
                try:
                    paramset['schema'] = self.__get_schema_uri(schema)
                except Exception as err:
                    raise err
            else:
                if key in required_keys:
                    mytardis[key] = expt_dict.pop(key)
                elif key == 'users':
                    users = expt_dict.pop(key)
                elif key == 'groups':
                    groups = expt_dict.pop(key)
                else:
                    params[key] = expt_dict.pop(key)
        parameter_list = []
        for key in params.keys():
            parameter_list.append({u'name': key,
                                   u'value': params[key]})
        paramset['parameters'] = parameter_list
        return (mytardis, paramset, users, groups)

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
            response = self.do_get_request('schema',
                                           params=query_params)
        except Exception as err:
            raise err
        else:
            schema_dict = json.loads(response.text)
            if schems_dict == []:
                logger.warning(
                    f'Schema {namespace} cannot be found in the database.')
                raise Exception(f'Schema {namespace} was not found in the database')
            elif len(schema_dict['objects']) > 1:
                logger.error(
                    f'Multiple instances of schema {namespace} found in the database. Please verify and clean up.')
                raise Exception(f'Multiple instances of schema {namespace} found in the database. Please verify and clean up.')
            else:
                schema = schema_dict['objects'][0]
                logger.debug(f'schema: {schema} found in the database.')
                return schema['resource_uri']

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
            response = self.do_get_request('experiment',
                                           params=query_params)
        except Exception as err:
            raise err
        else:
            resp_dict = json.loads(response.text)
            if resp_dict['objects'] == []:
                return False
            elif len(resp_dict['objects']) > 1:
                logger.error(f'More than one experiment with internal_id = {internal_id} exist in the database. Please investigate uniqueness of internal_id field')
                raise Exception(f'More than one experiment with internal_id = {internal_id} exist in the database. Please investigate uniqueness of internal_id field')
            else:
                obj = resp_dict['objects'][0]
                logger.debug(obj)
                return obj['resource_uri']

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
        proxies / http and https proxies if needed to pass through the firewall
        
        Returns:
        =================================
        True and the URI if the experiment is created successfully
        False and the URI if the experiment already exists in the database as determined from internal_id
        False and None empty dictionary if creation fails.
        '''
        os.chdir(self.root_dir)
        required_keys = ['title',
                         'internal_id',
                         'schema_namespace']
        check = check_dictionary(expt_dict,
                                 required_keys)
        if not check[0]:
            logger.error(f'The experiment dictionary is incomplete. Missing keys: {", ".join(check[1])}')
            return (False, None)
        try:
            uri = self.__get_experiment_uri(expt_dict['internal_id'])
        except Exception as err:
            return (False, None)
        if uri:
            return (False, uri)
        else:
            try:
                mytardis, paramset, users, groups = self.__build_experiment_dictionaries(expt_dict,
                                                                                         required_keys)
            except Exception as err:
                return (False, None)
            mytardis_json = dict_to_json(mytardis)
            try:
                response = self.do_post_request('experiment',
                                                expt_json)
                response.raise_for_status()
            except Exception as err:
                logger.error(f'Error occurred when creating experiment {mytardis["title"]}. Error: {err}')
                return (False, None)
            response = json.loads(response.text)
            uri = response['resource_uri']
            paramset['experiment'] = uri
            paramset_json = dict_to_json(paramset)
            try:
                response = self.do_post_request('experimentparameterset',
                                                parameter_set_json)
                response.raise_for_status()
            except Exception as err:
                logger.error(f'Error occurred when attaching metadata to experiment {mytardis["internal_id"]}. Error: {err}')
                return (True, uri)
            if groups:
                for group in groups:
                    try:
                        group_uri = self.__get_group_uri(group)
                    except Exception as err:
                        logger.error(f'Error: {err} occured when allocating group {group} access to experiment')
                        return (True, uri)
                        
            
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
