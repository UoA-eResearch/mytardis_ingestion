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
                 config_dict,
                 harvester):
        self.harvester = harvester
        self.digest_file = config_dict['digest_file']
        self.md5s = self.read_digest()
        pass

    @abstractmethod
    def upload_file(self):
        pass

    def read_digest(self):
        return_dict = {}
        try:
            with open(self.digest_file, 'r') as f:
                for line in f:
                    data = line.split('\t')
                    return_dict[data[0]] = {}
                    return_dict[data[0]]['md5'] = data[1]
                    if len(data) > 2:
                        return_dict[data[0]]['etag'] = data[2]
        except FileNotFoundError:
            return {}
        except Exception:
            # TODO - tidy up here
            pass
    
    def build_digest(self,
                     orig_filepath):
        if orig_filepath in self.md5s.keys():
            return True
        self.md5s['orig_filepath'] = {}
        local_md5sum = hlp.calculate_checksum(orig_filepath,
                                              md5sum_executable = self.harvester.md5sum_executable,
                                              subprocess_size_threshold = self.harvester.subprocess_size_threshold)
        self.md5s['orig_filepath']['md5'] = local_md5sum
        return True
