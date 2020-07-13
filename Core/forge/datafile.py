# Class to ingest project data into MyTardis

from .ingestor import MyTardisUploader
import logging

logger = logging.getLogger(__name__)

class DatafileForge(MyTardisUploader):

    def __init__(self,
                 global_config_file_path,
                 local_config_file_path,
                 checksum_digest=None):
        super().__init__(global_config_file_path,
                         local_config_file_path,
                         checksum_digest)

    def prepare_input():
        pass

    def get_or_create():
        pass

    def attach_parameters():
        pass
