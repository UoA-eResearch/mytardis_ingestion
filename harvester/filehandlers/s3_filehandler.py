'''Amazon s3 uploader scripts

Uploads a file list to s3 storage prior to linking back for myTardis ingestion.

Written by: Chris Seal
email: c.seal@auckland.ac.nz
'''

__author__ = 'Chris Seal <c.seal@auckland.ac.nz>'

from . import FileHandler
import boto3
import os
import sys
import hashlib
import logging
import subprocess
from ..helper import constants as CONST
from ..helper import helper as hlp
from pathlib import Path

logger = logging.getLogger(__name__)

class S3FileHandler(FileHandler):

    def __init__(self,
                 config_dict):
        self.s3_root_dir = config_dict['s3_root_dir']
        self.staging_dir = config_dict['staging_dir']
        self.bucket = config_dict['bucket']
        endpoint_url = config_dict['endpoint_url']
        if 'threshold' in config_dict.keys():
            self.threshold = config_dict['threshold']
        else:
            self.threshold = 1* CONST.GB
        if 'blocksize' in config_dict.keys():
            self.blocksize = config_dict['blocksize']
        else:
            self.blocksize = 1* CONST.GB
        self.cwd = os.getcwd()
        self.s3_client = boto3.client('s3',
                                      endpoint_url = endpoint_url)

    # =================================
    #
    # Code modified from https://alexwlchan.net/2019/07/listing-s3-leys/
    #
    #=================================
    def __get_matching_s3_objects(self, prefix="", suffix=""):
        """
        Generate objects in an S3 bucket.
        
        :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
        :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
        """
        s3 = self.s3_client
        paginator = s3.get_paginator("list_objects_v2")

        kwargs = {'Bucket': self.bucket}

        # We can pass the prefix directly to the S3 API.  If the user has passed
        # a tuple or list of prefixes, we go through them one by one.
        if isinstance(prefix, str):
            prefixes = (prefix, )
        else:
            prefixes = prefix

        for key_prefix in prefixes:
            kwargs["Prefix"] = key_prefix

            for page in paginator.paginate(**kwargs):
                try:
                    contents = page["Contents"]
                except KeyError:
                    break

                for obj in contents:
                    key = obj["Key"]
                    if key.endswith(suffix):
                        yield obj

    def __get_matching_s3_keys(self, prefix="", suffix=""):
        """
        Generate the keys in an S3 bucket.

        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        """
        for obj in get_matching_s3_objects(bucket, prefix, suffix):
            yield obj["Key"]
    # =================================
    #
    # End of code from web
    #
    # =================================

    def upload_file(self,
                    datafile_dict,
                    checksum_digest):
        pass

    def get_file_location(self,
                          datafile):
        pass
