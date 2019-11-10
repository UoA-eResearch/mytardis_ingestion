# Abstract FileHandler class as a base for the specialised FileHandlers needed
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#

__author__ = 'Chris Seal <c.seal@auckland.ac.nz>'

from abc import ABC, abstractmethod 
import logging

logger = logging.getLogger(__name__)

class FileHandler(ABC):

    @abstractmethod
    def __init__(self,
                 config_dict,
                 harvester):
        self.harvester = harvester
        pass

    @abstractmethod
    def upload_file(self):
        pass
