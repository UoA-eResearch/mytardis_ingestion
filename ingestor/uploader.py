#!/usr/bin/env python
# UoA MyTardis Uploader
# Written by Chris Seal <c.seal@auckland.ac.nz>
# Adapted from original ngs_ingestor script
# Thanks Andrew Perry <Andrew.Perry@monash.edu>
# Steve Androulakis <steve.androulakis@monash.edu>
# Grischa Meyer <grischa.meyer@monash.edu> for initial scripts

__author__ = 'Chris Seal <c.seal@auckland.ac.nz>'

import json
import requests
import logging
import sys
from urllib.parse import urljoin, urlparse
import backoff
from requests.auth import AuthBase
from helper import helper as hlp
import datetime
import os
import hashlib
import mimetypes
import subprocess
#import six

DEBUG = True
logger = logging.getLogger('mytardis')
if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
PROXY = True
proxies = {"http": 'http://squid.auckland.ac.nz:3128',
           'https': 'https://squid.auckland.ac.nz:3128'}

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
        self.verify_certificate: Flag telling MyTardis to verify the security certificate'''

        required_keys = ['server',
                         'username',
                         'api_key',
                         'root_dir']
        check = hlp.check_dictionary(config_dict,
                                     required_keys)
        if not check[0]:
            logger.critical(f'Config dictionary is incomplete. Missing keys: {", ".join(check[1])}')
            sys.exit()
        else:
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
            if 'store_dir' in config_dict.keys():
                self.store_dir = config_dict['store_dir']
            else:
                self.store_dir = None
            # Not sure that we want to force this but lets see if it breaks
            self.verify_certificate = True

    # Public Functions
    # =================================

    def do_post_request(self, action, data, extra_headers=None):
        '''Wrapper around self.__do_rest_api_request to handle POST requests

        Inputs:
        =================================
        action: the type of object, (e.g. experiment, dataset) to POST
        data: a JSON string holding the data to generate the object
        extra_headers: any additional information needed in the header (META) for the object being created
        
        Returns:
        =================================
        A Python requests module response object'''
        return self.__do_rest_api_request('POST',
                                          action,
                                          data=data,
                                          extra_headers=extra_headers)

    def do_get_request(self, action, params, extra_headers=None):
        '''Wrapper around self.__do_rest_api_request to handle GET requests

        Inputs:
        =================================
        action: the type of object, (e.g. experiment, dataset) to GET
        params: parameters to pass to filter the request return
        extra_headers: any additional information needed in the header (META) for the object being created
        
        Returns:
        =================================
        A Python requests module response object'''
        return self.__do_rest_api_request('GET',
                                          action,
                                          params=params,
                                          extra_headers=extra_headers)
    
    def create_experiment(self, expt_dict):
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
        False and an empty dictionary if creation fails.
        '''
        os.chdir(self.root_dir)
        users = None
        groups = None
        from datetime import datetime
        required_keys = ['title',
                         'internal_id',
                         'schema_namespace']
        check = hlp.check_dictionary(expt_dict,
                                     required_keys)
        if not check[0]:
            logger.error(f'The experiment dictionary is incomplete. Missing keys: {", ".join(check[1])}')
            return (False, {})
        else:
            # Unpack the expt_dict into mytardis dictionary which creates the experiment, users and
            # groups which define access to the experiment, and params which holds the metadata
            mytardis = {}
            for key in required_keys:
                if key == 'schema_namespace':
                    schema = expt_dict.pop(key)
                    schema_uri = self.__get_schema_by_name(schema)
                    if schema_uri == -1:
                        logger.error(f'Unable to uniquley identify schema {schema}. Please check database')
                        return (False, {})
                    elif not schema:
                        logger.error(f'Schema {schema} not present in the database.')
                        return (False, {})
                else:
                    mytardis[key] = expt_dict.pop(key)
            optional = {'project_id':'No Project',
                        'institution_name':'University of Auckland',
                        'description': 'No description',
                        'created_time': datetime.utcnow()}
            for key in optional.keys():
                if key in expt_dict.keys():
                    mytardis[key] = expt_dict.pop(key)
                else:
                    mytardis[key] = optional[key]
            # TODO - handle authors - needs us to expose ExptAuthor model
            params = {}
            if 'users' in expt_dict.keys():
                users = expt_dict.pop('users')
            if 'groups' in expt_dict.keys():
                groups = expt_dict.pop('groups')
            for key in expt_dict.keys():
                params[key] = expt_dict[key]
        # Check to see if the experiment already exists, if so return the URI
        uri = self.__get_experiment_uri(mytardis['internal_id'])
        if uri == -1:
            logger.error(f'Unable to create experiment due to clash of unique ID.')
            return (False, {})
        elif uri:
            return (False, uri)
        else:
            expt_json = hlp.dict_to_json(mytardis)
            try:
                response = self.do_post_request('experiment', expt_json)
                response.raise_for_status()
            except Exception as err:
                logger.error(f'Error occurred when creating experiment {mytardis["title"]}. Error: {err}')
                return (False, {})
            response_dict = json.loads(response.text)
            uri = response_dict['resource_uri']
            parameter_list = []
            for pkey in params.keys():
                parameter_list.append({u'name': pkey,
                                       u'value': params[pkey]})
            parameter_set = {'experiment': uri,
                             'schema': schema_uri,
                             'parameters': parameter_list}
            parameter_set_json = hlp.dict_to_json(parameter_set)
            try:
                response = self.do_post_request('experimentparameterset',
                                                parameter_set_json)
                response.raise_for_status()
            except Exception as err:
                logger.error(f'Error occurred when attaching metadata to experiment {mytardis["internal_id"]}. Error: {err}')
                return (True, uri)
            if groups is not None:
                for group in groups:
                    group_uri = self.__get_group_by_name(group)
                    if group_uri == -1:
                        logger.warning(f'Unable to add group {group} to the experiment {mytardis["internal_id"]} as the name is not unique in the database')
                    elif group_uri:
                        logger.info(f'Adding group {group} to the experiment {mytardis["internal_id"]}.')
                        try:
                            response = self.share_experiment_with_group(uri,
                                                                        group_uri)
                            response.raise_for_status()
                        except Exception as err:
                            logger.error(f'Error occurred when attaching group {group} to experiment {mytardis["internal_id"]}. Error: {err}')
                            return (True, uri)
                    else:
                        logger.warning(f'Unable to find group {group} in the database, so not adding group {group} to experiment {mytardis["internal_id"]}')
            if users is not None:
                for user in users:
                    user_uri = self.__get_user_by_name(user)
                    if user_uri == -1:
                        logger.warning(f'Unable to add user{user} to the experiment {mytardis["internal_id"]} as the name is not unique in the database')
                    elif user_uri:
                        logger.info(f'Adding user {user} to the experiment {mytardis["internal_id"]}.')
                        try:
                            response = self.share_experiment_with_user(uri,
                                                                       user_uri)
                            response.raise_for_status()
                        except Exception as err:
                            logger.error(f'Error occurred when attaching user {user} to experiment {mytardis["internal_id"]}. Error: {err}')
                            return (True, uri)
                    else:
                        logger.warning(f'Unable to find user {user} in the database, so not adding user {user} to experiment {mytardis["internal_id"]}')
            os.chdir(self.cwd)
            return (True, uri)

    def share_experiment_with_group(self, experiment_uri, group_uri,
                                    *args, **kwargs):
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
                                      *args,
                                      **kwargs)

    def share_experiment_with_user(self, experiment, user_uri, *args, **kwargs):
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
        return self.__share_experiment(experiment,
                                      'django_user',
                                      user_uri,
                                      *args,
                                      **kwargs)

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
        False and an empty dictionary if creation fails.
        '''
        os.chdir(self.root_dir)
        from datetime import datetime
        required_keys = ['internal_id',
                         'schema_namespace',
                         'description',
                         'dataset_id']
        check = hlp.check_dictionary(dataset_dict,
                                     required_keys)
        if not check[0]:
            logger.error(f'The dataset dictionary is incomplete. Missing keys: {", ".join(check[1])}')
            return (False, {})
        else:
            mytardis = {}
            for key in required_keys:
                if key == 'internal_id':
                    uri = self.__get_experiment_uri(dataset_dict.pop(key))
                    if uri == -1:
                        logger.critcal(f'Experiment ID {dataset_dict["internal_id"]} is not unique in the database.')
                        return (False, {})
                    elif not uri:
                        logger.error(f'Experiment ID {dataset_dict["internal_id"]} not found in the database, skipping.')
                        return (False, {})
                    else:
                        mytardis['experiments'] = [uri]
                elif key == 'schema_namespace':
                    schema = dataset_dict.pop(key)
                    schema_uri = self.__get_schema_by_name(schema)
                    if schema_uri == -1:
                        logger.error(f'Unable to uniquely identify schema {schema}. Please check database')
                        return (False, {})
                    elif not schema:
                        logger.error(f'Schema {schema} not present in the database.')
                        return (False, {})
                else:                  
                    mytardis[key] = dataset_dict.pop(key)
            optional = {'instrument': None,
                        'created_time': datetime.utcnow()}
            for key in optional.keys():
                if key in dataset_dict.keys():
                    if key == 'instrument':
                        if 'facility' in dataset_dict.keys():
                            facility = dataset_dict.pop('facility')
                        else:
                            facility = None
                        instrument = self.__get_instrument_by_name(dataset_dict.pop(key),
                                                                   facility)
                        if instrument:
                            mytardis[key] = instrument
                    else:
                        mytardis[key] = dataset_dict.pop(key)
                else:
                    mytardis[key] = optional[key]     
            params = {}
            for key in dataset_dict.keys():
                params[key] = dataset_dict[key]
            data_json = hlp.dict_to_json(mytardis)
            uri = self.__get_dataset_uri(mytardis['dataset_id'])
            if uri == -1:
                logger.error(f'Unable to create dataset due to clash of unique ID.')
                return (False, {})
            elif uri:
                return (False, uri)
            else:
                try:
                    response = self.do_post_request('dataset', data_json)
                    response.raise_for_status()
                except Exception as err:
                    logger.error(f'Error occurred when creating dataset {mytardis["description"]}. Error: {err}')
                    return (False, {})
            response_dict = json.loads(response.text)
            dataset_id = response_dict['resource_uri']
            parameter_list = []
            for pkey in params.keys():
                parameter_list.append({u'name': pkey,
                                       u'value': params[pkey]})
            parameter_set = {'dataset': dataset_id,
                             'schema': schema_uri,
                             'parameters': parameter_list}
            parameter_set_json = hlp.dict_to_json(parameter_set)
            response = self.do_post_request('datasetparameterset',
                                            parameter_set_json)
        os.chdir(self.cwd)
        return (True, dataset_id)
    
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
        in_store / A boolean flag indicating whether the file exists in the object store
        file_name / The file name for the object to be stored
        
        either:

        rel_path / the relative path to the datafile object to be ingested

        or:
        
        store_path / the relative path to the object from the top level of the remote store
        bucket / the storage bucket in which the object is located (s3 storage)

        Returns:
        =================================
        True and the URI if the dataset is created successfully
        False and the URI if the dataset already exists in the database as determined from dataset_id
        False and an empty dictionary if creation fails.
        '''
        os.chdir(self.root_dir)
        required_keys = ['schema_namespace',
                         'dataset_id',
                         'in_store',
                         'file_name']
        check = hlp.check_dictionary(datafile_dict,
                                     required_keys)
        if not check[0]:
            logger.error(f'The datafile dictionary is incomplete. Missing keys: {", ".join(check[1])}')
            return (False, {})
        else:
            in_store = datafile_dict.pop('in_store')
        if in_store:
            required_keys = ['s3_path',
                             'bucket',
                             'rel_path']
        else:
            required_keys = ['rel_path']
        check = hlp.check_dictionary(datafile_dict,
                                     required_keys)
        if not check[0]:
            logger.error(f'The datafile dictionary is incomplete. Missing keys: {", ".join(check[1])}')
            return (False, {})
        else:
            mytardis = {}
            dataset_id = datafile_dict.pop('dataset_id')
            uri = self.__get_dataset_uri(dataset_id)
            if uri == -1:
                logger.critcal(f'Dataset ID {dataset_id} is not unique in the database.')
                return (False, {})
            elif not uri:
                logger.warning(f'Dataset ID {dataset_id} not found in the database, skipping.')
                return (False, {})
            else:
                dataset_uri = uri
            schema_name = datafile_dict.pop('schema_namespace')
            uri = self.__get_schema_by_name(schema_name)
            if uri == -1:
                logger.error(f'Unable to uniquley identify schema {schema_name}. Please check database')
                return (False, {})
            elif not uri:
                logger.warning(f'Schema {schema_name} not found in the database, skipping.')
                return (False, {})
            else:
                schema_uri = uri
            file_name = datafile_dict.pop('file_name')
            if in_store:
                s3_path = datafile_dict.pop('s3_path')
                bucket = datafile_dict.pop('bucket')
                rel_path = datafile_dict.pop('rel_path')
            else:
                rel_path = datafile_dict.pop('rel_path')                
            params = {}
            parameter_list = []
            for key in datafile_dict.keys():
                params[key] = datafile_dict[key]
            for pkey in params.keys():
                parameter_list.append({u'name': pkey,
                                       u'value': params[pkey]})
            parameter_set = {'schema': schema_uri,
                             'parameters': parameter_list}
            parameter_sets = [parameter_set]
            filename = os.path.join(rel_path, file_name)
            md5_checksum = self.__md5_file_calc(filename)
            try:
                md5_checksum = md5_checksum.decode('utf8')
            except AttributeError:
                pass
            if in_store:
                logger.debug(f'Pushing {file_name} to myTardis by location')
                uri = self.__add_datafile_by_location(file_name, rel_path, s3_path, dataset_uri, bucket, parameter_sets, md5_checksum)
                return uri
            else:
                logger.debug(f'Pushing {file_name} through MyTardis')
                uri = self.__push_datafile(file_name,
                                           file_path,
                                           dataset_uri,
                                           parameter_sets_list=parameter_sets,
                                           md5_checksum=md5_checksum)
                return uri

    def remove_object_acl(self, object_id):
        self.do_get_request('objectacl', {})

    # Private Functions
    # =================================

    def __json_request_headers(self):
        return {'Accept': 'application/json',
                'Content-Type': 'application/json',
                'User-Agent': self.user_agent}

    def __get_experiment_uri(self, internal_id):
        '''Uses REST API GET with an internal_id filter

        Inputs:
        =================================
        internal_id: unique identifier for experiment

        Returns:
        =================================
        False if the id is not found in the database
        -1 if more than one instance of the id is found in the database.
            This should never happen and indicates that the uniqueness of the
            field in the database has been compromised. Raise an error to report this
        URI of the experiment if a single instance of the id is found in the database
        '''
        query_params = {u'internal_id': internal_id}
        response = self.do_get_request('experiment',
                                       params=query_params)
        resp_dict = json.loads(response.text)
        logger.debug(f'Response: {resp_dict}')
        if resp_dict['objects'] == []:
            return False
        elif len(resp_dict['objects']) > 1:
            logger.error(f'More than one experiment with internal_id = {internal_id} exist in the database. Please investigate uniqueness of internal_id field')
            return -1
        else:
            obj = resp_dict['objects'][0]
            logger.debug(obj)
            return obj['resource_uri']

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
        headers = self.__json_request_headers()
        if extra_headers is not None:
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
        except requests.exceptions.RequestException as e:
            logger.error("Request failed : %s : %s", e.message, url)
            raise e
        except Exception as err:
            logger.error(f'Error, {err.message}, occurred when attempting to call api request {url}')
            raise err
        return response

    def __get_group_by_name(self, group):
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
        query_params = {'name': group}
        group_uris_filtered_by_name = self.do_get_request('group',
                                                        params=query_params)
        group_dict = json.loads(group_uris_filtered_by_name.text)
        if group_dict['objects'] == []:
            logger.warning(
                f'Group: {group} has not been added to the database.')
            return False
        elif len(group_dict['objects']) > 1:
            logger.error(
                f'Multiple instances of Group: {group} found in database. Please verify and clean up.')
            return -1
        else:
            logger.debug(f'Group: {group} found in the database.')
            group_uri = group_dict['objects'][0]
            return group_uri['resource_uri']

    def __get_user_by_name(self, username):
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
        user_uris_filtered_by_upi = self.do_get_request('user',
                                                        params=query_params)
        user_dict = json.loads(user_uris_filtered_by_upi.text)
        if user_dict['objects'] == []:
            logger.warning(
                f'UPI: {username} has not been added to the user database and the user cannot add or make changes.')
            return False
        elif len(user_dict['objects']) > 1:
            logger.error(
                f'Multiple instances of UPI: {upi} found in user database. Please verify and clean up.')
            return -1
        else:
            logger.debug(f'UPI: {username} found in the database.')
            user = user_dict['objects'][0]
            return user['resource_uri']

    def __get_instrument_by_name(self, name, facility=None):
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
            response = self.do_get_request("instrument",
                                       params=query_params)
            response.raise_for_status()
        except Exception as err:
            logger.error(f'Error occurred when creating dataset {mytardis["description"]}. Error: {err}')
            return False        
        instrument_dict = json.loads(response.text)
        if instrument_dict['objects'] == []:
            logger.warning(
                f'Instrument: {name} not found in list of instruments')
            return False
        elif len(instrument_dict['objects']) > 1:
            logger.warning(
                f'Cannot uniquely identify {name} in the list of instruments')
            return False
        else:
            logger.debug(f'Instrument {name} indentified')
            obj = instrument_dict['objects'][0]
            return obj['resource_uri']

    def __get_schema_by_name(self, name):
        '''Reads the database for schema and returns a resource_uri for one by name

        Inputs:
        =================================
        name: the schema namespace

        Returns:
        =================================
        False if there are no schema by that namepace in the database
        -1 if more than one schema by that namespace are found
        URI if one schema with the search namespace.
        '''
        query_params = {"namespace": name}
        schema_uris_filtered_by_name = self.do_get_request('schema',
                                                          params=query_params)
        schema_dict = json.loads(schema_uris_filtered_by_name.text)
        if schema_dict['objects'] == []:
            logger.warning(
                f'schema {name} cannot be found in the database.')
            return False
        elif len(schema_dict['objects']) > 1:
            logger.error(
                f'Multiple instances of schema {name} found in the database. Please verify and clean up.')
            return -1
        else:
            schema = schema_dict['objects'][0]
            logger.debug(f'schema: {schema} found in the database.')
            return schema['resource_uri']

    def __get_dataset_uri(self, dataset_id):
        '''Uses REST API GET with an dataset_id filter

        Inputs:
        =================================
        dataset_id: unique identifier for dataset

        Returns:
        =================================
        False if the id is not found in the database
        -1 if more than one instance of the id is found in the database.
            This should never happen and indicates that the uniqueness of the
            field in the database has been compromised. Raise an error to report this
        URI of the experiment if a single instance of the id is found in the database
        '''
        query_params = {u'dataset_id': dataset_id}
        response = self.do_get_request('dataset',
                                       params=query_params)
        resp_dict = json.loads(response.text)
        if resp_dict['objects'] == []:
            return False
        elif len(resp_dict['objects']) > 1:
            logger.error(f'More than one dataset with dataset_id = {dataset_id} exist in the database. Please investigate uniqueness of internal_id field')
            return -1
        else:
            obj = resp_dict['objects'][0]
            logger.debug(obj)
            return obj['resource_uri']

    def __add_datafile_by_location(self,
                                   file_name,
                                   rel_path,
                                   s3_path,
                                   dataset_uri,
                                   bucket = None,
                                   parameter_sets_list=None,
                                   md5_checksum=None):
        '''Creates a datafile object in myTardis with a link to the object in remote storage

        Inputs:
        =================================
        file_name: the name of the file in object storage
        rel_path: 
        s3_path: the file path of the file in object store relative to the research directory
        dataset_uri: the uri to the dataset to which the file belongs
        bucket: the s3 bucket in which the file resides
        parameter_sets_list: a list of parameter sets holding the metadata for the file
        md5_checksum: the md5 sum for the file

        Returns:
        =================================
        False: The datafile could not be successfully ingested
        URI: The uri of the datafile
        '''
        if bucket is None:
            bucket = self.bucket
        if md5_checksum is None:
            md5_checksum = '__undetermined__'
        directory = os.path.normpath(s3_path)
        if directory == '.':
            directory = ''
        filename = os.path.join(rel_path, file_name)
        file_size = os.path.getsize(filename)
        file_dict = {
            u'dataset': dataset_uri,
            u'filename': file_name,
            u'directory': s3_path,
            u'mimetype': mimetypes.guess_type(filename)[0],
            u'size': file_size,
            u'parameter_sets': parameter_sets_list,
            u'md5sum': md5_checksum,
            u'verified': True,
            u'replicas': [{
                u'uri': os.path.join(s3_path,file_name),
                u'location': 'tardis', #storage_box - taken from mytardis admin,
                u'protocol': "file"}]
        }
        data = hlp.dict_to_json(file_dict)
        headers = self.__json_request_headers()
        try:
            response = self.do_post_request('dataset_file',
                                            data,
                                            extra_headers=headers)
            response.raise_for_status()
        except Exception as err:
            logger.error(f'Error occurred when creating datafile {filename}. Error: {err}')
            return False
        return True
        
    def __push_datafile(self,
                        file_name,
                        file_path,
                        dataset_uri,
                        parameter_sets_list=None,
                        md5_checksum='__undetermined__'):
        '''Function to push a data file through myTardis' interface

        Inputs:
        =================================
        file_name: the file name
        rel_path: the file path relative to the top-level directory
        dataset_uri: the uri for the dataset that the file will be attached to
        parameter_sets_list: a list of parameter sets containing the metadata for the file
        md5_checksum: a md5 sum for the file

        Results:
        =================================
        False if the datafile could not be ingested
        URI: the uri of the file in myTardis
        '''
        
        directory = os.path.normpath(file_path)
        if directory == '.':
            directory = ''
        filename = os.path.join(file_path, file_name)
        if md5_checksum is None:
            md5_checksum = '__undetermined__'
        file_size = os.path.getsize(filename)
        file_dict = {
            u'dataset': dataset_uri,
            u'filename': file_name,
            u'md5sum': md5_checksum,
            u'directory': directory,
            u'mimetype': mimetypes.guess_type(filename)[0],
            u'size': file_size,
            u'parameter_sets': parameter_sets_list
        }
        from requests_toolbelt import MultipartEncoder
        data = hlp.dict_to_json(file_dict)
        with open(filename, 'rb') as f:
            form = MultipartEncoder(fields={'json_data': data,
                                            'attached_file': ('text/plain', f)})
            headers = self.__json_request_headers()
            headers['Content-Type'] = form.content_type
            try:
                response = self.do_post_request('dataset_file',
                                            form,
                                            extra_headers=headers)
                response.raise_for_status()
            except Exception as err:
                logger.error(f'Error occurred when creating datafile {filename}. Error: {err}')
                return False
            response_dict = json.loads(response.text)
            return response_dict['resource_uri']

    def __md5_file_calc(self, file_path, blocksize=None,
                    subprocess_size_threshold=10*1024*1024,
                    md5sum_executable='/usr/bin/md5sum'):
            
        if os.path.getsize(file_path) > subprocess_size_threshold and \
           os.path.exists(md5sum_executable) and \
           os.access(md5sum_executable, os.X_OK):
            return self.__md5_subprocess(file_path,
                                         md5sum_executable=md5sum_executable)
        else:
            return self.__md5_python(file_path, blocksize=blocksize) 

    def __md5_python(self, file_path, blocksize=None):
        """
        Calculates the MD5 checksum of a file, returns the hex digest as a
        string. Streams the file in chunks of 'blocksize' to prevent running
        out of memory when working with large files.
        #
        :type file_path: string
        :type blocksize: int
        :return: The hex encoded MD5 checksum.
        :rtype: str"""
        if not blocksize:
            blocksize = 128

        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(blocksize)
                if not chunk:
                    break
                md5.update(chunk)
        return md5.hexdigest()

    def __md5_subprocess(self, file_path,
                         md5sum_executable='/usr/bin/md5sum'):
        """
        Calculates the MD5 checksum of a file, returns the hex digest as a
        string. Streams the file in chunks of 'blocksize' to prevent running
        out of memory when working with large files.
        #
        :type file_path: string
        :return: The hex encoded MD5 checksum.
        :rtype: str"""
        out = subprocess.check_output([md5sum_executable, file_path])
        checksum = out.split()[0]
        if len(checksum) == 32:
            return checksum
        else:
            raise ValueError('md5sum failed: %s', out)

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

    def __share_experiment(self,
                          content_object,
                          plugin_id,
                          entity_object,
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
        import six
        if isinstance(content_object, six.string_types):
            object_id = self.__resource_uri_to_id(content_object)
        elif isinstance(content_object, int):
            object_id = content_object
        else:
            raise TypeError("'content_object' must be a URL string or int ID")
        if isinstance(entity_object, six.string_types):
            entity_id = self.__resource_uri_to_id(entity_object)
        elif isinstance(entity_object, int):
            entity_id = enitity_object
        else:
            raise TypeError("'entity_object' must be a URL string or int ID")
        acl_ownership_type = self.__get_ownership_int(acl_ownership_type)
        data = {
            u'pluginId': plugin_id,
            u'entityId': str(entity_id),
            u'content_type': str(content_type),
            u'object_id': str(object_id),
            u'aclOwnershipType': acl_ownership_type,
            u'isOwner': True,
            u'canRead': True,
            u'canWrite': True,
            u'canDelete': False,
            u'effectiveDate': None,
            u'expiryDate': None
        }
        response = self.do_post_request('objectacl', hlp.dict_to_json(data))
        return response
    
    def __raise_request_exception(self, response):
        e = requests.exceptions.RequestException(response=response)
        e.message = "%s %s" % (response.status_code, response.reason)
        raise e
