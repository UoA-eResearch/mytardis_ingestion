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
from decouple import Config, RepositoryEnv
from pathlib import Path
import shutil

logger = logging.getLogger(__name__)

class S3FileHandler(FileHandler):

    def __init__(self,
                 global_config_file_path,
                 local_config_file_path,
                 checksum_digest=None):
        # global_config holds environment variables that don't change often such as LDAP parameters and project_db stuff
        global_config = Config(RepositoryEnv(global_config_file_path))
        # local_config holds the details about how this particular set of data should be handled
        local_config = Config(RepositoryEnv(local_config_file_path))
        # s3_root_dir should be ProjectID/ExptID/DatasetID
        self.s3_root_dir = Path(local_config('FILEHANDLER_REMOTE_ROOT'))
        # This is a temp solution to the streaming timeout problem
        self.staging_dir = Path(local_config('FILEHANDLER_STAGING_ROOT'))
        origin = Path(local_config('FILEHANDLER_ORIGIN_ROOT',
                                   default=None))
        self.bucket = local_config('FILEHANDLER_S3_BUCKET')
        endpoint_url = local_config('FILEHANDLER_S3_ENDPOINT_URL')
        self.threshold = local_config('FILEHANDLER_S3_THRESHOLD')
        self.blocksize = local_config('FILEHANDLER_BLOCKSIZE')
        self.cwd = os.getcwd()
        self.s3_client = boto3.client('s3',
                                      endpoint_url = endpoint_url)
        self.checksums = {}
        self.checksum_digest = checksum_digest
        if self.checksum_digest:
            self.checksums = readJSON(self.checksum_digest)
        self.move_to_staging(origin)

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
                                   rel_path):
        '''
        Function to read the checksum_digest (in memory as dictionary)
        and to extract the etag from the calculated checksums

        Inputs:
        =================================
        data_file_dict: A datafile dictionary.

        Returns:
        =================================
        checksum or None: a string containing the etag checksum or None if its not in the
            dictionary.
        '''
        if rel_path in self.checksums.keys():
            if 'etag' in self.checksums[rel_path].keys():
                return checksum[rel_path]['etag']
            else:
                etag = hlp.calculate_etag(rel_path)
                self.checksums[relpath]['etag'] = etag
                return etag
        else:
            self.checksums = hlp.build_checksum_digest(self.checksum_digest,
                                                       self.staging_dir,
                                                       rel_path,
                                                       s3 = True,
                                                       s3_blocksize = self.blocksize)
            return self.checksums[rel_path]['etag']

    def upload_file(self,
                    file_path):
        '''Wrapper around self.__multipart_upload function'''
        rel_path = Path(file_path).relative_to(self.staging_dir)
        etag = self.__get_checksum_from_digest(rel_path)
        try:
            response = self.__multipart_upload(rel_path,
                                               etag)
        except Exception as error:
            raise error
        return response

    def move_to_staging(self,
                        origin):
        shutil.copytree(origin, self.staging_dir)
