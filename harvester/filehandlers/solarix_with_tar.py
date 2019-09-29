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
                 config_dict):
        super().__init__(config_dict)

    def create_method_tar_file(self,
                               m_dir):
        parent = m_dir.parent
        tar_file = parent / (parent.name[:-2] + "_method.tar.gz")
        tf = tarfile.open(tar_file, mode='w:gz')
        cwd = os.getcwd()
        os.chdir(m_dir)
        for child in m_dir.iterdir():
            if child.is_file():
                tf.add(child.name)
        tf.close()
