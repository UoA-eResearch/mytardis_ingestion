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
from ..helper import constants as CONST

logger = logging.getLogger(__name__)

class S3FileHandler(FileHandler):

    def __init__(self,
                 config_dict):
        super().__init__(config_dict)
        self.s3_root_dir = config_dict['s3_root_dir']
        self.local_root_dir = config_dict['local_root_dir']
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

    def __multipart_upload(self,
                           local_location_path,
                           file_name,
                           s3_location_path):
        '''Uploads a file to a specified location in an s3 bucket.bucket
        If the file size is above the threshold size, then the file is uploaded
        as a multi-part upload.

        Inputs:
        =================================
        local_location_path: The relative file path (relative to the root file directory).
        file_name: The file name.
        s3_location_path: the location in the s3 object store to save the file.
            This is a relative path underneath the s3_root_dir.

        Returns:
        =================================
        True if file is uploaded correctly,
        False if the file does not upload or if the ETag differs from that calculated
        '''
        s3_location_path = os.path.join(self.s3_root_dir, s3_location_path)
        file_path = os.join(self.local_root_dir, local_location_path, file_name)
        from boto3.s3.transfer import TransferConfig
        config = TransferConfig(multipart_threshold=self.threshold,
                                multipart_chunksize=self.blocksize,
                                max_io_queue = 1)
        self.s3_client.upload_file(file_path,
                                   self.bucket,
                                   s3_location_path,
                                   Config=config)
        try:
            s3_header = self.__get_file_header(s3_location_path,
                                               bucket)
            if s3_header:
                etag = self.__checkETag(file_path,
                                      test_etag = s3_header['ETag'])
            else:
                error_message = f'Upload of {file_path} to {s3_location_path} did not complete successfully'
                logger.warning(error_message)
                raise FileNotFoundError(error_message)
        except FileNotFoundError:
            return False
        except ValueError:
            logger.warning(f'S3 ETags do not match. Check that file {file_path} has correctly uploaded to {s3_location_path}')
            return False
        except Exception as e:
            logger.error(traceback.format_exc())
            return False
        return True

    def __get_file_header(self,
                          s3_location_path):
        '''Checks to see if the file exists in s3 object storage and returns the
        header if it does

        Inputs:
        =================================
        s3_location_path: The location in the s3 object store to look for the file
        bucket: s3 object store bucket name, defaults to the bucket defined during
            class initialisation

        Returns:
        =================================
        False if the file does not exist
        s3_header: Dictionary containg the file header if the file is found
        '''
        s3_location_path = os.path.join(self.s3_root_dir, s3_location_path)
        logger.debug(f'Looking for file in {s3_location_path}')
        response = self.s3_client.list_objects_v2(Bucket=self.bucket,
                                                  Prefix=s3_location_path)
        for obj in response.get('Contents', []):
            if obj['Key'] == s3_location_path:
                return obj
        logger.warning(f'File {s3_location_path} was not found in the object store')
        return False

    def __checkETag(self,
                    file_path,
                    test_etag = None):
        '''Calculates the S3 ETag (md5 sum unless multipart upload). If a test_etag is
        provided, then the calculated ETag is compared against the test value. If there
        is a difference in the two then raises a ValueError exception.

        Inputs:
        =================================
        file_path: The file to calclate the s3 ETag from.
        test_etag: The test value to compare against (Optional)

        Returns:
        =================================
        new_etag: The calculated s3 ETag
        '''
        md5s = []
        if os.path.getsize(file_path) > self.threshold: # multipart upload
            try:
                with open(file_path, 'rb') as f:
                    flg = True
                    while flg:
                        data = f.read(self.blocksize)
                        if not data:
                            flg = False
                        else:
                            md5s.append(hashlib.md5(data))
                    if len(md5s) > 1:
                        digests = b"".join(m.digest() for m in md5s)
                        new_md5 = hashlib.md5(digests)
                        new_etag =f'"{new_md5.hexdigest()}-{len(md5s)}"'
                    elif len(md5s) == 1: # if blocksize is larger than threshold then warn
                        logger.warning(f'The blocksize for the upload of {file_path} is larger than the multipart upload threshold. Please check this.')
                        new_etag = f'"{md5s[0].hexdigest()}"'
                    else:
                        logger.warning(f'An empty file has been considered for multipart uploading.')
                        new_etag = '""'
            except FileNotFoundError:
                logger.warning(f'File {file_path} not found when calculating the s3 ETag.')
                raise ValueError(f'No s3 ETag calculated as file {file_path} not found.')
            except Exception as e:
                raise
        else:
            try:
                chunk_size = 100 * CONST.MB
                md5 = hashlib.md5()
                with open(file_path, 'rb') as f:
                    for chunk in iter(lambda: f.read(chunk_size), b""):
                        md5.update(chunk)
                new_etag = md5.hexdigest()
            except FileNotFoundError:
                logger.warning(f'File {file_path} not found when calculating the s3 ETag.')
                raise ValueError(f'No s3 ETag calculated as file {file_path} not found.')
            except Exception as e:
                raise
        if test_etag:
            if not new_etag == test_etag:
                logger.warning(f'S3 ETag calculated from {file_path} is {new_etag}, which differs from the test ETag {test_etag}. Please check file upload')
                raise ValueError(f'S3 ETag calculated from {file_path} is different from the test ETag')
        return new_etag
        
    def upload_file(self,
                    file_dict):
        '''Wrapper around self.__multipart_upload function'''
        s3_location_path = file_dict['remote_dir']
        local_location_path = os.path.join(self.local_root_dir, file_dict['local_dir'])
        file_name = file_dict['file_name']
        response = self.__multipart_upload(local_location_path,
                                           file_name,
                                           s3_location_path)
        if not response:
            logger.error(f'File {file_name} was not successfully uploaded into s3 object store. Please check this log for further details.')
            # TODO: add email warning to logger
        return response
