# s3_filehandler.py
#
# S3 filehandling class
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 06 Aug 2020
#

import boto3
from pathlib import Path
from mytardis_helper.mt_json import readJSON
from checksum import calculate_etag
from config_helper import process_config
import logging
import traceback

logger = logging.getLogger('__name__')

boto3.set_stream_logger('')


def convert_pathstring_to_Path(string):
    return Path(string)


def convert_pathstrings_in_dictionary(input_dict,
                                      keys):
    for key in keys:
        input_dict[key] = convert_pathstring_to_Path(input_dict[key])
    return input_dict


class S3FileHandler():

    def __init__(self,
                 local_config):

        local_keys = [
            'staging_root',
            'remote_root',
            'bucket',
            'endpoint_url',
            'threshold',
            'blocksize'
        ]

        self.config = process_config(keys=local_keys,
                                     local_filepath=local_config)
        # convert path's to Path objects
        pathkeys = [
            'remote_root',
            'staging_root']
        self.config = convert_pathstrings_in_dictionary(self.config,
                                                        pathkeys)
        self.s3_client = boto3.client('s3',
                                      endpoint_url=self.config['endpoint_url'])
        print(self.s3_client)

    # =================================
    #
    # Code modified from https://alexwlchan.net/2019/07/listing-s3-keys/
    #
    # =================================
    def get_matching_s3_objects(self,
                                prefix="",
                                suffix=""):
        """
        Generate objects in an S3 bucket.

        :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
        :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
        """
        s3 = self.s3_client
        paginator = s3.get_paginator("list_objects_v2")
        kwargs = {'Bucket': self.config['bucket']}

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

    def get_matching_s3_keys(self,
                             prefix="",
                             suffix=""):
        """
        Generate the keys in an S3 bucket.

        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        """
        for obj in self.get_matching_s3_objects(prefix,
                                                suffix):
            yield obj["Key"]
    # =================================
    #
    # End of code from web
    #
    # =================================

    def get_obj_from_store(self,
                           filepath):
        """
        Note: filepath should be relative to the s3 root
        """
        fullpath = self.config['remote_root'] / filepath
        objs = []
        for obj in self.get_matching_s3_objects(fullpath.as_posix()):
            objs.append(obj)
        if len(objs) > 1:
            error_msg = f'Multiple files with the same name and file path found in object store'
            logger.critical(error_msg)
            raise Exception(error_msg)
        elif len(objs) == 1:
            return objs[0]
        else:
            return False

    def check_etag_on_obj(self,
                          obj,
                          checksum):
        etag = obj['ETag']
        if etag == checksum:
            return True
        else:
            return False

    def upload_to_object_store(self,
                               filepath):
        remote_path = self.config['remote_root'] / filepath
        local_path = self.config['staging_root'] / filepath
        from boto3.s3.transfer import TransferConfig
        transfer_config = TransferConfig(multipart_threshold=int(self.config['threshold']),
                                         multipart_chunksize=int(
                                             self.config['blocksize']),
                                         max_io_queue=1)
        try:
            self.s3_client.upload_file(local_path.as_posix(),
                                       self.config['bucket'],
                                       remote_path.as_posix(),
                                       Config=transfer_config)
        except Exception as error:
            logger.error(traceback.format_exc())
            return None
