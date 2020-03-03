# Functions to create RaIDs and to update them as necessary (once it is working)

import logging
import requests
from requests.auth import AuthBase
from decouple import Config, RepositoryEnv
from urllib.parse import urljoin, quote
import backoff
from .helper import dict_to_json

logger = logging.getLogger(__name__)

class RAiDAuth(AuthBase):

    def __init__(self,
                 api_key):
        self.api_key = api_key

    def __call__(self,
                 request):
        request.headers['Authorization'] = f'Bearer {self.api_key}'
        print(request.headers)
        return request

class RAiDFactory():

    def __init__(self,
                 global_config_file_path):
        # global_config holds environment variables that don't change often such as LDAP parameters and project_db stuff
        global_config = Config(RepositoryEnv(global_config_file_path))
        self.api_key = global_config('RAID_API_KEY')
        self.url_base = global_config('RAID_URL')
        self.auth = RAiDAuth(self.api_key)
        self.proxies = {"http": global_config('PROXY_HTTP',
                                              default=None),
                        "https": global_config('PROXY_HTTPS',
                                               default=None)}
        if self.proxies['http'] == None and self.proxies['https'] == None:
            self.proxies = None
        self.verify_certificate = global_config('RAID_VERIFY_CERT',
                                                default=True,
                                                cast=bool)

    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.Timeout,
                           requests.exceptions.ConnectionError),
                          max_tries=8)
    def __rest_api_call(self,
                        method,  # REST api method
                        action,  # action here refers to experiment, dataset or datafile
                        data=None,
                        params=None,
                        extra_headers=None):
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
        url = self.url_base + f'/{action}'
        print(url)
        headers = {'Accept': 'application/json'}
        if extra_headers:
            headers = {**headers, **extra_headers}
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
            logger.error("Request failed : %s : %s", err, url)
            raise err
        except Exception as err:
            logger.error(f'Error, {err.msg}, occurred when attempting to call api request {url}')
            raise err
        return response
        
    def mint_project_raid(self,
                          project_url,
                          project_name,
                          project_description,
                          project_metadata=None,
                          project_startdate=None):
        from datetime import datetime
        raid_dict = {}
        raid_dict['contentPath'] = project_url
        if project_startdate:
            raid_dict['startDate'] = project_startdate
        raid_dict['meta'] = {'name': project_name,
                             'description': project_description}
        if project_metadata:
            for key in project_metadata.keys():
                raid_dict['meta'][key] = project_metadata[key]
        raid_json = dict_to_json(raid_dict)
        response = self.__rest_api_call('POST',
                                        'RAiD',
                                        data=raid_json)
        return response

    def get_project_raid(self,
                         raid_handle):        
        url_safe_raid_handle = quote(raid_handle, safe='')
        response = self.__rest_api_call('GET',
                                        f'RAiD/{url_safe_raid_handle}')
        return response

    def __mint_raid(self,
                    name,
                    description,
                    url,
                    metadata,
                    startdate):
        from datetime import datetime
        raid_dict = {}
        raid_dict['contentPath'] = url
        if startdate:
            raid_dict['startDate'] = startdate
        raid_dict['meta'] = {'name': name,
                             'description': description}
        if metadata:
            for key in metadata.keys():
                if not 'meta' in raid_dict.keys():
                    raid_dict['meta'] = {}
                raid_dict['meta'][key] = metadata[key]
        raid_json = dict_to_json(raid_dict)
        print(raid_json)
        response = self.__rest_api_call('POST',
                                        'RAiD',
                                        data=raid_json)
        return response

    def mint_experiment_raid(self,
                             experiment_name,
                             experiment_description,
                             experiment_url='https://mytardis.nectar.auckland.ac.nz/experiment',
                             experiment_metadata=None,
                             experiment_startdate=None):
        try:
            resp = self.__mint_raid(experiment_name,
                                    experiment_description,
                                    experiment_url,
                                    experiment_metadata,
                                    experiment_startdate)
        except Exception as error:
            raise
        return resp
           
    def mint_dataset_raid(self,
                          dataset_description,
                          dataset_url,
                          dataset_metadata=None,
                          dataset_startdate=None):
        try:
            resp = self.__mint_raid(dataset_description,
                                    'UoA MyTardis Dataset',
                                    dataset_url,
                                    dataset_metadata,
                                    dataset_startdate)
        except Exception as error:
            raise
        return resp

    def mint_project_raid(self,
                          project_name,
                          project_description,
                          project_url,
                          project_metadata=None,
                          project_startdate=None):
        try:
            resp = self.__mint_raid(project_name,
                                    project_description,
                                    project_url,
                                    project_metadata,
                                    project_startdate)
        except Exception as error:
            raise
        return resp

    def update_raid(self,
                    url,
                    name,
                    description,
                    raid_handle):
        raid_dict = {'contentPath': url,
                     'name': name,
                     'description': description}
        raid_json = dict_to_json(raid_dict)
        url_safe_raid_handle = quote(raid_handle, safe='')
        response = self.__rest_api_call('PUT',
                                        f'RAiD/{url_safe_raid_handle}',
                                        data=raid_json)
        return response


