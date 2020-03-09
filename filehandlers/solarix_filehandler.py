'''Solarix specific s3 uploader.

Creates a tar file containing the contents of the .m directories.

Written by: Chris Seal
email.c.seal@auckland.ac.nz
'''

__author__ = 'Chris Seal <c.seal@auckland.ac.nz>'

from . import S3FileHandler
import boto3
import os
from helper import zip_directory
import re
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class SolarixFileHandler(S3FileHandler):

    def __init__(self,
                 global_config_file_path,
                 local_config_file_path,
                 checksum_digest = None):
        super().__init__(global_config_file_path,
                         local_config_file_path,
                         checksum_digest)
        
    def upload_dir(self,
                   d_dir):
        for root, directories, files in os.walk(d_dir):
            for filename in files:
                if filename[-4:] == '.zip':
                    continue
                else:
                    abs_filepath = Path(os.path.join(root, filename))
                    response = abs_filepath.as_posix() #self.upload_file(abs_filepath)
                    print(response)
        return response
