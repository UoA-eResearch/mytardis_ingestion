# Functions to create RaIDs and to update them as necessary (once it is working)

import logging
import requests
from decouple import Config, RepositoryEnv
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

DEBUG=True

class RaIDFactory():

    def __init__(self,
                 global_config_file_path,
                 local_config_file_path):
        # global_config holds environment variables that don't change often such as LDAP parameters and project_db stuff
        global_config = Config(RepositoryEnv(global_config_file_path))
        # local_config holds the details about how this particular set of data should be handled
        local_config = Config(RepositoryEnv(local_config_file_path))
        self.api_key = global_config('RAID_API_KEY')
        self.url_base = global_config('RAID_URL') 
        self.url_template = urljoin(self.url_base,
                                    '/%s/')

    def mint_project_raid(self,
                          project_url):
        # TODO CODE TO MINT PROJECT RAID
