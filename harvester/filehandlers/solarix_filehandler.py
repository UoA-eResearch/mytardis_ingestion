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

class SolarixFileHandler(S3FileHandler):

    def __init__(self,
                 config_dict,
                 harvester):
        super().__init__(config_dict, harvester)

    def create_method_tar_file(self,
                               m_dir):
        local_parent = m_dir.parent
        rel_parent = local_parent.relative_to(self.root_dir)
        staging_parent = self.staging_dir / rel_parent
        tar_file = staging_parent / (staging_parent.name[:-2] + "_method.tar.gz")
        tf = tarfile.open(tar_file, mode='w:gz')
        cwd = os.getcwd()
        os.chdir(m_dir)
        for child in m_dir.iterdir():
            if child.is_file():
                tf.add(child.name)
        tf.close()
