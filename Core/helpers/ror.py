# RORFactory classes
#
# Wrapper classes around the Python Requests library
# to facilitate accessing RORs
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#
# Updated 23 Jul 2020

import logging
import requests
from requests.auth import AuthBase
from decouple import Config, RepositoryEnv
from urllib.parse import urljoin, quote
import backoff
from .mt_json import dict_to_json

logger = logging.getLogger(__name__)


class RORFactory():

    def __init__(self,
                 global_config_file_path):
        # global_config holds environment variables that don't change often such as LDAP parameters and project_db stuff
        global_config = Config(RepositoryEnv(global_config_file_path))
        self.url_base = global_config('ROR_URL')
        self.proxies = {"http": global_config('PROXY_HTTP',
                                              default=None),
                        "https": global_config('PROXY_HTTPS',
                                               default=None)}
        if self.proxies['http'] == None and self.proxies['https'] == None:
            self.proxies = None

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
                                            proxies=self.proxies)
            else:
                response = requests.request(method,
                                            url,
                                            data=data,
                                            params=params,
                                            headers=headers)
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

    def get_ror(self,
                ror_handle):
        url_safe_raid_handle = quote(ror_handle, safe='')
        response = self.__rest_api_call('GET',
                                        f'organizations/{url_safe_raid_handle}')
        return response
