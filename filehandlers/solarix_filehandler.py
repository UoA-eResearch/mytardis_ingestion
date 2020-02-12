'''Solarix specific s3 uploader.

Creates a tar file containing the contents of the .m directories.

Written by: Chris Seal
email.c.seal@auckland.ac.nz
'''

__author__ = 'Chris Seal <c.seal@auckland.ac.nz>'

from . import S3FileHandler
import boto3
import os
import tarfile
import re
import logging

logger = logging.getLogger(__name__)

class SolarixFileHandler(S3FileHandler):

    def __init__(self,
                 config_dict,
                 harvester):
        super(SolarixFileHandler, self).__init__(config_dict, harvester)

    def create_method_tar_file(self,
                               m_dir):
        local_parent = m_dir.parent
        rel_parent = local_parent.relative_to(self.harvester.root_dir)
        staging_parent = self.staging_dir / rel_parent
        tar_name = '_'
        for part in staging_parent.parts:
            if re.match(r'\w*_\d{6}.d', part):
                part_list = part.split('_')
                del part_list[-1]
                tar_name = tar_name.join(part_list)
        if tar_name == '_':
            tar_name = staging_parent.name[:-2]
        tar_file = staging_parent / (tar_name + "_method.tar.gz")
        tar_file.parent.mkdir(parents=True, exist_ok=True)
        tf = tarfile.open(tar_file, mode='w:gz')
        cwd = os.getcwd()
        os.chdir(m_dir)
        for child in m_dir.iterdir():
            if child.is_file():
                tf.add(child.name)
        tf.close()
        
    def upload_file(self,
                    file_dict):
        '''Wrapper around self.__multipart_upload function'''
        s3_location_path = file_dict['remote_dir']
        #local_location_path = os.path.join(self.harvester.root_dir, file_dict['local_dir'])
        staging_location_path = os.path.join(self.staging_dir, file_dict['local_dir'])
        file_name = file_dict['file']
        if file_name[-14:] != '_method.tar.gz':
            response = self._S3FileHandler__move_file_to_staging(file_dict['local_dir'],
                                             file_name)
            if not response:
                logger.error(f'File {file_name} was not successfully staged for uploading')
                raise FileNotFoundError(f'File {file_name} was not successfully staged for uploading')
        else:
            staging_location_path = os.path.join(self.staging_dir, file_dict['local_dir'].parent)
        if file_dict['local_dir'] in self.harvester.files_dict.keys():
            self.harvester.files_dict[file_dict['local_dir']].remove(file_name)
        
        response = self._S3FileHandler__multipart_upload(staging_location_path,
                                                         file_name,
                                                         s3_location_path)
        if not response:
            logger.error(f'File {file_name} was not successfully uploaded into s3 object store. Please check this log for further details.')
            # TODO: add email warning to logger
        else:
            open_file = open(self.harvester.processed_file_path, 'w')
            for key in self.harvester.files_dict.keys():
                if self.harvester.files_dict[key] == []:
                    open_file.write(f'{self.harvester.root_dir}/{key}\n')
            open_file.close()
        return response
