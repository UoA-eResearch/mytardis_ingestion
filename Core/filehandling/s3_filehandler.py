# s3_filehandler.py
#
# S3 filehandling class
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 07 Aug 2020
#

import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from pathlib import Path
from ..helpers import process_config
from smart_open import open
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
                 local_config,
                 staging_root,
                 remote_root=None):

        local_keys = [
            's3_key',
            's3_secret_key',
            'endpoint_url',
            'threshold',
            'blocksize'
        ]

        self.config = process_config(keys=local_keys,
                                     local_filepath=local_config)
        self.config['blocksize'] = int(self.config['blocksize'])
        # convert path's to Path objects
        self.config['staging_root'] = staging_root
        self.config['remote_root'] = remote_root
        if remote_root:
            pathkeys = [
                'remote_root',
                'staging_root']
        else:
            pathkeys = ['staging_root']
        self.config = convert_pathstrings_in_dictionary(self.config,
                                                        pathkeys)
        self.s3_session = boto3.Session(
            aws_access_key_id=self.config['s3_key'],
            aws_secret_access_key=self.config['s3_secret_key']
        )
        self.s3_client = self.s3_session.client(
            's3',
            aws_session_token=None,
            region_name='us-east-1',
            use_ssl=True,
            endpoint_url=self.config['endpoint_url'],
            config=None
        )

    # =================================
    #
    # Code modified from https://alexwlchan.net/2019/07/listing-s3-keys/
    #
    # =================================
    def get_matching_s3_objects(self,
                                bucket,
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
        kwargs = {'Bucket': bucket}

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
                             bucket,
                             prefix="",
                             suffix=""):
        """
        Generate the keys in an S3 bucket.

        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        """
        for obj in self.get_matching_s3_objects(bucket,
                                                prefix,
                                                suffix):
            yield obj["Key"]
    # =================================
    #
    # End of code from web
    #
    # =================================

    def get_obj_from_store(self,
                           bucket,
                           filepath,
                           remote_root=None):
        """
        Note: filepath should be relative to the s3 root
        """
        if not remote_root:
            remote_root = self.config['remote_root']
        fullpath = remote_root / filepath
        objs = []
        for obj in self.get_matching_s3_objects(bucket,
                                                fullpath.as_posix()):
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

    def list_buckets(self):
        try:
            response = self.s3_client.list_buckets()
        except ClientError as err:
            logging.error(err)
            return []
        except Exception as err:
            logging.error(err)
            raise
        buckets = response['Buckets']
        bucket_list = []
        for bucket in buckets:
            bucket_list.append(bucket['Name'])
        return bucket_list

    def list_files(self,
                   bucket):
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket)
        except ClientError as err:
            logging.error(err)
            return []
        except Exception as err:
            logging.error(err)
            raise
        file_list = []
        for obj in response['Contents']:
            file_list.append(obj['Key'])
        return file_list

    def create_bucket(self,
                      bucket):
        buckets = self.list_buckets()
        if bucket in buckets:
            return False
        try:
            self.s3_client.create_bucket(Bucket=bucket)
        except Exception as err:
            logging.error(err)
            raise
        return True

    def read_in_chunks(self,
                       file_object):
        '''
        Iterator to read a file chunk by chunk.
        file_object: file opened by caller
        '''
        while True:
            data = file_object.read(self.config['blocksize'])
            if not data:
                break
            yield data

    def copy_file_to_new_bucket(self,
                                src_bucket,
                                src_key,
                                dst_bucket,
                                dst_key=None):
        try:
            cpy_src = '{0}/{1}'.format(src_bucket, src_key)
            self.s3_client.copy_object(Bucket=dst_bucket,
                                       CopySource=cpy_src,
                                       Key=dst_key)
        except Exception as err:
            logging.error(err)
            print(err)
            raise err

    def remove_object(self,
                      bucket,
                      key):
        self.s3_client.delete_object(Bucket=bucket,
                                     Key=KeyError)

    def upload_to_object_store(self,
                               filepath,
                               bucket,
                               remote_root=None,
                               staging_root=None):
        if not remote_root:
            remote_root = self.config['remote_root']
        if not staging_root:
            staging_root = self.config['staging_root']
        if remote_root:
            remote_path = remote_root / filepath
        else:
            remote_path = filepath
        local_path = staging_root / filepath
        #size = local_path.stat().st_size
        #multipart = size > self.config['blocksize']
        '''s3_uri = 's3://{0}/{1}'.format(bucket,
                                       remote_path)
        try:
            with open(local_path, 'rb') as file_input:
                with open(s3_uri,
                          'wb',
                          transport_params={
                              'session': self.s3_session,
                              'buffer_size': self.config['blocksize'],
                              'multipart_upload': multipart,
                              'resource_kwargs':
                              {
                                  'endpoint_url': self.config['endpoint_url']
                              }
                          },
                          ignore_ext=True) as s3_destination:
                    for chunk in self.read_in_chunks(file_input):
                        s3_destination.write(chunk)
        except Exception as error:
            logger.error(traceback.format_exc())
            return None'''
        config = TransferConfig(multipart_threshold=self.config['blocksize'],
                                multipart_chunksize=self.config['blocksize'],
                                max_io_queue=1)
        try:
            self.s3_client.upload_file(local_path.as_posix(),  # the file to upload
                                       bucket,  # the bucket to put it into
                                       remote_path,  # the s3 file path
                                       Config=config)
        except ClientError as error:
            logger.error(error)
            return None
