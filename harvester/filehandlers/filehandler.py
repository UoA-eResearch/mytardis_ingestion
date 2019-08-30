# Abstract FileHandler class as a base for the specialised FileHandlers needed
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#


from abc import ABC, abstractmethod
import logging
import os
import hashlib
import subprocess

logger = logging.getLogger(__name__)

class FileHandler(ABC):

    @abstractmethod
    def __init__(self,
                 config_dict):
        self.md5sum_executable = '/usr/bin/md5sum'
        self.subprocess_size_threshold = 10*1024*1024
        self.blocksize = 128
        pass

    @abstractmethod
    def upload_file(self):
        pass

    def __calculate_checksum(self,
                             file_dir,
                             file_name):
        file_path = os.path.join(file_dir, file_name)
        if os.path.getsize(file_path) > self.subprocess_size_threshold and \
           os.path.exists(self.md5sum_executable) and \
           os.access(self.md5sum_executable, os.X_OK):
            try:
                md5sum = self.__md5_subprocess(file_path,
                                               md5sum_executable=self.md5sum_executable)
            except ValueError:
                md5sum = self.__md5_python(file_path, blocksize=self.blocksize)
            except Exception as err:
                logger.error(f'Error: {err} arose when determining MD5 checksum for file {file_name}')
                raise
        else:
            try:
                md5sum = self.__md5_python(file_path, blocksize=self.blocksize)
            except Exception as err:
                logger.error(f'Error: {err} arose when determining MD5 checksum for file {file_name}')
                raise
        return md5sum

    def __md5_python(self, file_path, blocksize=None):
        """
        Calculates the MD5 checksum of a file, returns the hex digest as a
        string. Streams the file in chunks of 'blocksize' to prevent running
        out of memory when working with large files.
        #
        :type file_path: string
        :type blocksize: int
        :return: The hex encoded MD5 checksum.
        :rtype: str"""
        if not blocksize:
            blocksize = 128

        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(blocksize)
                if not chunk:
                    break
                md5.update(chunk)
        return md5.hexdigest()

    def __md5_subprocess(self, file_path,
                         md5sum_executable='/usr/bin/md5sum'):
        """
        Calculates the MD5 checksum of a file, returns the hex digest as a
        string. Streams the file in chunks of 'blocksize' to prevent running
        out of memory when working with large files.
        #
        :type file_path: string
        :return: The hex encoded MD5 checksum.
        :rtype: str"""
        out = subprocess.check_output([md5sum_executable, file_path])
        checksum = out.split()[0]
        if len(checksum) == 32:
            return checksum
        else:
            raise ValueError('md5sum failed: %s', out)
