# Abstract FileHandler class as a base for the specialised FileHandlers needed
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#

__author__ = 'Chris Seal <c.seal@auckland.ac.nz>'

from abc import ABC, abstractmethod
from ..helper import helper as hlp
import logging

logger = logging.getLogger(__name__)

class FileHandler(ABC):

    @abstractmethod
    def __init__(self,
                 config_dict):
        pass

    @abstractmethod
    def upload_file(self,
                    datafile_dict,
                    checksum_digest):
        pass

