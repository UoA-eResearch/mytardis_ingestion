# Abstract FileHandler class as a base for the specialised FileHandlers needed
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#

__author__ = 'Chris Seal <c.seal@auckland.ac.nz>'

from abc import ABC, abstractmethod
from ..helper import constants as CONST 
import logging

logger = logging.getLogger(__name__)

class FileHandler(ABC):

    @abstractmethod
    def __init__(self,
                 config_dict):
        self.md5sum_executable = confing_dict['md5sum_executable']
        self.subprocess_size_threshold = 100*CONST.MB
        self.blocksize = 128
        pass

    @abstractmethod
    def upload_file(self):
        pass
