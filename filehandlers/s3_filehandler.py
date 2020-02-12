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
        # s3_root_dir should be ProjectID/ExptID/DatasetID
        self.s3_root_dir = config_dict['s3_root_dir']
        # This is a temp solution to the streaming timeout problem
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
    # Code modified from https://alexwlchan.net/2019/07/listing-s3-keys/
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
        for obj in get_matching_s3_objects(prefix, suffix):
            yield obj["Key"]
    # =================================
    #
    # End of code from web
    #
    # =================================

    def __get_eTag(self,
                   obj):
        """Returns the ETag from a file in the object store.
        The function carries out no error checking and the number of objects
        needs to be checked outside this function.
        
        =================================
        Inputs:
        =================================
        obj: an object returned from the object store

        =================================
        Returns:
        =================================
        etag: The ETag calculated for the object in the store - see S3
        documentation for details."""
        return obj['ETag']

    def __get_obj(self,
                  rel_file_path):
        s3_path = self.s3_root_dir / rel_file_path
        objs = []
        for obj in self.__get_matching_s3_objects(s3_path):
            objs.append(obj)
        if len(objs) > 1:
            error_msg = f'Multiple files with the same name and file path found in object store'
            logger.critical(error_msg)
            raise Exception(error_msg)
        elif len(objs) == 1:
            return objs[0]
        elif objs == []:
            return False
        else:
            error_msg = f'Negative number of objects added to list. Something is very wrong'
            logger.critical(error_msg)
            raise Exception(error_msg)

    def __check_checksum(self,
                         rel_file_path,
                         checksum):
        obj = self.__get_obj(rel_file_path)
        if not obj:
            return None
        etag = self.__get_eTag(obj)
        if etag == checksum:
            return True
        else:
            return False

    def __check_checksum_on_obj(self,
                                obj,
                                checksum):
        etag = self.__get_etag(obj)
        if etag == checksum:
            return True
        else:
            return False


    def __multipart_upload(self,
                           rel_file_path,
                           checksum):
        '''Uploads a file to a specified location in an s3 bucket.bucket
        If the file size is above the threshold size, then the file is uploaded
        as a multi-part upload.

        Inputs:
        =================================
        rel_file_path: A Path object, the relative file path (relative to the root file directory).

        Returns:
        =================================
        True if file is uploaded correctly,
        False if the ETag differs from that calculated
        None if the file did not upload
        '''
        s3_path = self.s3_root_dir / rel_file_path
        # This is the local location
        file_path = self.staging_dir / rel_file_path
        from boto3.s3.transfer import TransferConfig
        config = TransferConfig(multipart_threshold=self.threshold,
                                multipart_chunksize=self.blocksize,
                                max_io_queue = 1)
        try:
            self.s3_client.upload_file(file_path.as_posix(), # the file to upload
                                       self.bucket, # the bucket to put it into
                                       s3_path.as_posix(), # the s3 file path
                                       Config=config)
        except ClientError as error:
            logger.error(error)
            return None
        try:
            obj = self.__get_obj(s3_path)
            if obj:
                check = self.__check_checksum_on_obj(obj,
                                                     checksum)
            else:
                error_msg = f'Upload of {rel_file_path.as_posix()} to {s3_path.as_posix()} did not complete successfully'
                logger.error(error_msg)
                return None
        except Exception as error:
            logger.error(traceback.format_exc())
            return None
        return check

    def __get_checksum_from_digest(self,
                                   rel_path,
                                   checksum_digest):
        '''
        Function to read the checksum_digest (in memory as dictionary)
        and to extract the etag from the calculated checksums

        Inputs:
        =================================
        data_file_dict: A datafile dictionary.
        checksum_digest: A dictionary of checksums keyed to relative file paths

        Returns:
        =================================
        checksum or None: a string containing the etag checksum or None if its not in the
            dictionary.
        '''
        if rel_path in checksum_digest.keys():
            return checksum_digest[rel_path]['etag']
        else:
            return None

    def upload_file(self,
                    datafile_dict,
                    checksum_digest):
        '''Wrapper around self.__multipart_upload function'''
        rel_path = datafile_dict['rel_path']
        etag = self.__get_checksum_from_digest(rel_path,
                                               checksum_digest)
        if not etag:
            file_path = self.staging_dir / rel_file_path
            checksum = hlp.calculate_etag(file_path,
                                          self.blocksize)
        try:
            response = self.__multipart_upload(rel_path,
                                               checksum)
        except Exception as error:
            raise error
        return response
