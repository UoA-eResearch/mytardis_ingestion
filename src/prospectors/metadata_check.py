"""
Checks and validates metadata according to a profile, to ensure all the fields
are present that are required in dataclasses.
"""

# ---Imports
import copy
import logging

from src.profiles import output_manager as om
from src.profiles.profile_selector import ProfileSelector
from typing import Optional

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class MetadataCheck:

    """Checks for common or known operating system files or file prefixes
    that are not normally intended for ingestion into MyTardis.
    """

    def __init__(
        self,
    ) -> None:
        pass

    def check_metadata_in_path(
        self,
        profile_sel: ProfileSelector,
        path: str,
        recursive: bool = True,
        out_man: Optional[om.OutputManager] = None,
    ) -> om.OutputManager:
        if out_man:
            new_out_man = copy.deepcopy(out_man)
        else:
            new_out_man = om.OutputManager()

        custom_prospector = profile_sel.load_custom_prospector()
        prspctr = custom_prospector.CustomProspector()
        new_out_man = prspctr.inspect(path, recursive, new_out_man)

        return new_out_man