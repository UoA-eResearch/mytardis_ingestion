# Functions to create RaIDs and to update them as necessary (once it is working)

import logging
import requests
from requests.auth import AuthBase
from decouple import Config, RepositoryEnv
from urllib.parse import urljoin, quote
import backoff

logger = logging.getLogger(__name__)

DEBUG=True

class RaIDAuth(AuthBase):

    def __init__(self,
                 api_key):
        self.api_key = api_key

    def __call__(self,
                 request):
        request.headers['Authorization'] = f'Bearer {self.api_key}'
        print(request.headers)
        return request

class RaIDFactory():

    def __init__(self,
                 global_config_file_path):
        # global_config holds environment variables that don't change often such as LDAP parameters and project_db stuff
        global_config = Config(RepositoryEnv(global_config_file_path))
        self.api_key = global_config('RAID_API_KEY')
        self.url_base = global_config('RAID_URL')
        self.auth = RaIDAuth(self.api_key)
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
                          requests.exceptions.RequestException,
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
                          project_metadata=None,
                          project_startdate=None):
        # TODO CODE TO MINT PROJECT RAID
        from datetime import datetime
        raid_dict = {}
        raid_dict['contentPath'] = project_url
        if project_startdate:
            raid_dict['startDate'] = project_startdate
        if project_metadata:
            for key in project_metadata.keys():
                if not 'meta' in raid_dict.keys():
                    raid_dict['meta'] = {}
                raid_dict['meta'][key] = project_metadata[key]
        response = self.__rest_api_call('POST',
                                        'RAiD',
                                        data=raid_dict)

    def get_project_raid(self,
                         raid_handle):        
        url_safe_raid_handle = quote(raid_handle, safe='')
        response = self.__rest_api_call('GET',
                                               f'RAiD/{url_safe_raid_handle}')
        print(response.json())
        return response


