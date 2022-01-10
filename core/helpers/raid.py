# RAiDAuth and RAiDFactory classes
#
# Wrapper classes around the Python Requests library
# to facilitate the creation and updating of RAiDs
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#
# Updated 23 Jul 2020

import logging
import requests
from requests.auth import AuthBase
from urllib.parse import quote
import backoff
from .mt_json import dict_to_json
from .config_helper import process_config

logger = logging.getLogger(__name__)


class RAiDAuth(AuthBase):

    def __init__(self,
                 api_key):
        self.api_key = api_key

    def __call__(self,
                 request):
        request.headers['Authorization'] = f'Bearer {self.api_key}'
        return request


class RAiDFactory():

    def __init__(self,
                 global_config_file_path):
        # global_config holds environment variables that don't change often
        # such as LDAP parameters and project_db stuff
        local_keys = [
            'raid_api_key',
            'raid_url',
            'raid_cert',
            'proxy_http',
            'proxy_https']
        self.config = process_config(keys=local_keys,
                                     global_filepath=global_config_file_path)
        self.auth = RAiDAuth(self.config['raid_api_key'])
        print(self.config)
        self.proxies = {}
        if 'proxy_http' in self.config.keys():
            self.proxies["http"] = self.config['proxy_http']
        if 'proxy_https' in self.config.keys():
            self.proxies["https"] = self.config['proxy_https']
        if self.proxies is {}:
            self.proxies = None
        self.verify_certificate = bool(self.config['raid_cert'])

    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.Timeout,
                           requests.exceptions.ConnectionError),
                          max_tries=8)
    def rest_api_call(self,
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
        url = self.config['raid_url'] + f'/{action}'
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
            logger.error(
                f'Error, {err.msg}, occurred when attempting to call api request {url}')
            raise err
        return response

    def get_all_raids(self):
        response = self.rest_api_call('GET',
                                      'RAiD?owner=true')
        return response

    def get_raid(self,
                 raid_handle):
        url_safe_raid_handle = quote(raid_handle, safe='')
        response = self.rest_api_call('GET',
                                      f'RAiD/{url_safe_raid_handle}')
        return response

    def mint_raid(self,
                  name,
                  description,
                  url,
                  metadata=None,
                  startdate=None):
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
        response = self.rest_api_call('POST',
                                      'RAiD',
                                      data=raid_json)
        return response

    def update_raid(self,
                    name,
                    description,
                    url,
                    raid_handle):
        raid_dict = {'contentPath': url,
                     'name': name,
                     'description': description}
        raid_json = dict_to_json(raid_dict)
        url_safe_raid_handle = quote(raid_handle, safe='')
        response = self.rest_api_call('PUT',
                                      f'RAiD/{url_safe_raid_handle}',
                                      data=raid_json)
        return response
