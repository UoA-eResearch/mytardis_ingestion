# Abstract FileHandler class as a base for the specialised FileHandlers needed
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#

from abc import ABC, abstractmethod
from ..helper import calculate_checksum, md5_python, md5_subprocess
import logging

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
