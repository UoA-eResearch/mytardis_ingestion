# MyTardisREST class
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated 27 Jul 2020

from requests.auth import AuthBase
import backoff
import requests
from urllib.parse import urljoin
from .helpers import process_config


class MyTardisAuth(AuthBase):
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


class MyTardisRESTFactory():
    ''' Class to interact with MyTardis by calling the REST API'''

    user_agent_name = __name__
    user_agent_url = 'https://github.com/UoA-eResearch/mytardis_ingestion.git'

    def __init__(self,
                 local_config_file_path):
        config_keys = {'server': 'MYTARDIS_URL',
                       'ingest_user': 'MYTARDIS_INGEST_USER',
                       'ingest_api_key': 'MYTARDIS_INGEST_API_KEY',
                       'verify_certificate': 'MYTARDIS_VERIFY_CERT',
                       'proxy_http': 'PROXY_HTTP',
                       'proxy_https': 'PROXY_HTTPS'}
        config_dict = process_config(keys=config_keys,
                                     local_filepath=local_config_file_path)
        self.auth = MyTardisAuth(config_dict['ingest_user'],
                                 config_dict['ingest_api_key'])
        self.proxies = {'http': config_dict['proxy_http'],
                        'https': config_dict['proxy_https']}
        self.verify_certificate = config_dict['verify_certificate']
        self.api_template = urljoin(config_dict['server'],
                                    '/api/v1/%s/')
        self.user_agent = '%s/%s (%s)' % (self.user_agent_name,
                                          '2.0',
                                          self.user_agent_url)

    def __raise_request_exception(self, response):
        '''Function to add additional information to the base RequestException
        exception. Shoudl think about subclassing this.'''
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
        url = self.api_template % action
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/json',
                   'User-Agent': self.user_agent}
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
            raise err
        except Exception as err:
            raise err
        return response

    def get_request(self,
                    action,
                    params,
                    extra_headers=None):
        '''Wrapper around self._do_rest_api_request to handle GET requests

        Inputs:
        =================================
        action: the type of object, (e.g. experiment, dataset) to GET
        params: parameters to pass to filter the request return
        extra_headers: any additional information needed in the header (META) for the
        object being created

        Returns:
        =================================
        A Python requests module response object
        '''
        try:
            response = self.__rest_api_request('GET',
                                               action,
                                               params=params,
                                               extra_headers=extra_headers)
        except Exception as err:
            raise err
        return response

    def post_request(self,
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
