'''Amazon s3 uploader scripts

Uploads a file list to s3 storage prior to linking back for myTardis ingestion.

Written by: Chris Seal
email: c.seal@auckland.ac.nz
'''

__author__ = 'Chris Seal <c.seal@auckland.ac.nz>'

# Library imports:
import boto3
import os
import sys
import hashlib
import logging

# Global constants:
KB = 1024
MB = KB ** 2
GB = KB ** 3

DEBUG = False
logger = logging.getLogger(__name__)
if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

class S3Uploader():

    def __init__(self,
                 research_directory,
                 file_root_directory,
                 box_name,
                 endpoint_url = 'https://store.nectar.auckland.ac.nz:443',
                 bucket = 'mytardis',
                 threshold = 1*GB,
                 blocksize = 1*GB):
        '''Initialise uploader.
        
        Inputs:
        =================================
        research_directory: Top level directory name to create subtree under
        file_root_directory: Root directory on local storage for files
        endpoint_url: URL defining the location of the s3 object store
        bucket: s3 object store bucket name
        threshold: the minimum file size that will cause multi-part uploading to occur
        blocksize: multipart upload block size

        Returns:
        =================================
        Nil

        Members:
        =================================
        self.research_directory: Top level directory name in s3 object store
        self.file_root_directory: Root directory on local storage for files
        self.bucket: s3 object store bucket name
        self.threshold: threshold size for mutli-part uploading
        self.blocksize: blocksize for multi-part uploading
        self.s3_client: boto3 client for uploading pointing to object store
        '''
        cwd = os.getcwd()
        self.research_directory = research_directory
        self.file_root_directory = file_root_directory
        log_file = logging.FileHandler(os.path.join(cwd,'s3_upload.log'))
        logger.addHandler(log_file)
        self.bucket = bucket
        self.threshold = threshold
        self.blocksize = blocksize
        self.s3_client = boto3.client('s3',
                                      endpoint_url = endpoint_url)
        self.box_name = box_name
        
    # Private Functions
    # =================================

    def __multipart_upload(self,
                           file_path,
                           s3_location_path,
                           bucket = None):
        '''Uploads a file to a specified location in an s3 bucket.bucket
        If the file size is above the threshold size, then the file is uploaded
        as a multi-part upload.

        Inputs:
        =================================
        rel_path: The relative file path (relative to the root file directory).
        file_name: The file name.
        s3_location_path: the location in the s3 object store to save the file.
            This is a relative path underneath the research_directory.
        bucket: s3 object store bucket name, defaults to the bucket defined
            during class initialisation.

        Returns:
        =================================
        True if file is uploaded correctly,
        False if the file does not upload or if the ETag differs from that calculated
        '''
        if bucket is None:
            bucket = self.bucket
        #file_path = os.path.join(self.box_name, file_path)
        s3_location_path = os.path.join(self.box_name, s3_location_path)
        from boto3.s3.transfer import TransferConfig
        config = TransferConfig(multipart_threshold=self.threshold,
                                multipart_chunksize=self.blocksize,
                                max_io_queue = 1)
        self.s3_client.upload_file(file_path,
                                   bucket,
                                   s3_location_path,
                                   Config=config)
        try:
            s3_header = self.__get_file_header(s3_location_path,
                                               bucket)
            if s3_header:
                etag = self.__checkETag(file_path,
                                      test_etag = s3_header['ETag'])
            else:
                raise FileNotFoundError(f'Upload of {file_path} to {s3_location_path} did not complete successfully')
        except FileNotFoundError:
            logger.warning(f'Upload of {file_path} to {s3_location_path} did not complete successfully')
            return False
        except ValueError:
            logger.warning(f'S3 ETags do not match. Check that file {file_path} has correctly uploaded to {s3_location_path}')
            return False
        except Exception as e:
            logger.error(traceback.format_exc())
            return False
        return True

    def __get_file_header(self,
                          s3_location_path,
                          bucket = None):
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
        if bucket is None:
            bucket = self.bucket
        logger.debug(f'Looking for file in {s3_location_path}')
        response = self.s3_client.list_objects_v2(Bucket=bucket,
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
                chunk_size = 100 * MB
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

    # Public Functions
    # =================================

    def upload_file(self,
                    file_dict):
        '''Wrapper around self.__multipart_upload function'''
        if 'bucket' not in file_dict.keys():
            bucket = self.bucket
        else:
            bucket = file_dict['bucket']
        s3_location_path = os.path.join(file_dict['s3_path'],file_dict['file_name'])
        file_path = os.path.join(self.file_root_directory, file_dict['rel_path'], file_dict['file_name'])
        response = self.__multipart_upload(file_path,
                                           s3_location_path,
                                           bucket)
        if not response:
            logger.error(f'File {file_path} was not successfully uploaded into s3 object store. Please check this log for further details.')
            # TODO: add email warning to logger
        return response

    def upload_file_list(self,
                         file_dict_list):
        '''Reads a list of files and locations, typically generated from a CSV or JSON from the instrument
        side. Attempts to upload the files to s3 storage sequentially. Returns a list of files that have
        successfully been uploaded.

        Inputs:
        =================================
        file_dict: A dictionary of file paths to be uploaded and locations to upload to.

        Returns:
        =================================
        uploads: A list of successful uploads based on return from self.__multipart_upload
        '''
        uploads = []
        for file_dict in file_dict_list:
            if 'bucket' not in file_dict.keys():
                bucket = self.bucket
            else:
                bucket = file_dict['bucket']
            s3_location_path = os.path.join(file_dict['s3_path'],file_dict['file_name'])
            file_path = os.path.join(self.file_root_directory, file_dict['rel_path'], file_dict['file_name'])
            response = self.__multipart_upload(file_path,
                                               s3_location_path,
                                               bucket)
            if response:
                uploads.append(file_path)
        return uploads