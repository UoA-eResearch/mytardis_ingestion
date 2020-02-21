# Functions to read/write from the project database as necessary (once it is working)

import logging
import requests
from requests.auth import AuthBase
from decouple import Config, RepositoryEnv
from urllib.parse import urljoin, quote
import backoff
from .helper import dict_to_json
import json

logger = logging.getLogger(__name__)

class ProjectDBAuth(AuthBase):

    def __init__(self,
                 api_key):
        self.api_key = api_key

    def __call__(self,
                 request):
        request.headers['apikey'] = f'{self.api_key}'
        print(request.headers)
        return request

class ProjectDBFactory():

    def __init__(self,
                 global_config_file_path):
        # global_config holds environment variables that don't change often such as LDAP parameters and project_db stuff
        global_config = Config(RepositoryEnv(global_config_file_path))
        self.api_key = global_config('PROJECT_DB_API_KEY')
        self.url_base = global_config('PROJECT_DB_URL')
        self.auth = ProjectDBAuth(self.api_key)
        self.proxies = {"http": global_config('PROXY_HTTP',
                                              default=None),
                        "https": global_config('PROXY_HTTPS',
                                               default=None)}
        if self.proxies['http'] == None and self.proxies['https'] == None:
            self.proxies = None
        self.verify_certificate = global_config('PROJECT_DB_VERIFY_CERT',
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
        print(url)
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/json'}
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

    def get_mytardis_id(self,
                        mytardis_id):
        response = self.__rest_api_call('GET',
                                        f'mytardis/{mytardis_id}')
        return response

    def post_mytardis_experiment(self,
                                 mytardis_url,
                                 expt_raid):
        data_dict = {'mytardis_code':expt_raid,
                     'url':mytardis_url}
        data_json = dict_to_json(data_dict)
        response = self.__rest_api_call('POST',
                                        'mytardis',
                                        data=data_json)
        return response

    def get_project_from_code(self,
                              code):
        response = self.__rest_api_call('GET',
                                        f'project/findByCode/{code}')
        return response

    def get_people_from_project(self,
                                project_db_id):
        response = self.__rest_api_call('GET',
                                        f'project/{project_db_id}/member')
        return response

    def get_email_from_person_project_db_id(self,
                                            person_id):
        response = self.__rest_api_call('GET',
                                        f'person/{person_id}')
        return response.json()['email']

    def get_project_id_from_code(self,
                                   code):
        try:
            response = self.get_project_from_code(code)
        except Exception as error:
            logger.error(error.message)
            raise            
        return response.json()['id']

    def __get_person_id_from_uri(self,
                                 uri):
        data = uri.split('/')
        return data[-1]

    def get_people_ids_from_project(self,
                                    project_db_id,
                                    roles=None): # roles is a list of personrole numbers that will be
                                                 # used to limit types of people returned
        response = self.get_people_from_project(project_db_id)
        resp = response.json()
        ids = []
        for person in resp:
            current = person['person']['href']
            if roles:
                current_role = person['role']['href']
                current_role_id = self.__get_person_id_from_uri(current_role)
                if int(current_role_id) in roles or current_role_id in roles:
                    ids.append(self.__get_person_id_from_uri(current))
            else:
                ids.append(self.__get_person_id_from_uri(current))
        return ids
