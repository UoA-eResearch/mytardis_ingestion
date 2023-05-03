"""Performs the mining process

The mining process involves generating metadata files for MyTardis ingestion.
"""


# ---Imports
import copy
import logging
import os

from src.profiles import profile_selector
from src.profiles import output_manager as om


# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class Miner():
    
    def __init__(self,
                 profile: str,
                ) -> None:
        self.profile_sel = profile_selector.ProfileSelector(profile)


    def mine_directory(self,
                       path: str,
                       recursive: bool = True,
                       out_man: om.OutputManager = None,
                       ) -> om.OutputManager:
        if not out_man:
            new_out_man = om.OutputManager()
        else:
            new_out_man = copy.deepcopy(out_man)

        custom_miner = self.profile_sel.load_custom_miner()
        miner = custom_miner.CustomMiner()
        miner.mine(path, recursive, out_man)

        return new_out_man
