"""Inspects metadata and performs related checks on a path.
"""


# ---Imports
import copy
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.extraction_output_manager import output_manager as om
from src.profiles import profile_consts as pc
from src.profiles.abi_music import abi_music_consts as amc

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class MetadataProspector:
    """Inspects metadata and performs related checks on a path."""

    def __init__(
        self,
    ) -> None:
        return

    def check_metadata_for_mining(
        self,
        out_man: om.OutputManager,
        metadata_fp_list: list[str],
    ) -> om.OutputManager:
        """
        Checks metadata JSON files for missing fields and adds success or failure entries to the Output Manager.

        Args:
            out_man (om.OutputManager): Output Manager object.
            metadata_fp_list (list[str]): List of filepaths for metadata JSON files.

        Returns:
            om.OutputManager: Updated Output Manager object.
        """
        new_out_man = copy.deepcopy(out_man)
        for metadata_fp in metadata_fp_list:
            with open(metadata_fp, "r") as f:
                md_dict = json.load(f)
            missing_fields = self._search_for_missing_fields(md_dict)

            if len(missing_fields) == 0:
                new_out_man.add_success_entry_to_dict(
                    metadata_fp,
                    pc.PROCESS_PROSPECTOR,
                    "metadata is ready for ingestion",
                )
            else:
                is_still_ingestable = True
                for field in missing_fields:
                    if field not in amc.FIELDS_WITH_DEFAULTS_LUT:
                        is_still_ingestable = False
                        break

                if is_still_ingestable:
                    msg = "metadata is ready for ingestion, but will have default values assigned for missing fields"
                    new_out_man.add_success_entry_to_dict(
                        metadata_fp, pc.PROCESS_PROSPECTOR, msg
                    )
                else:
                    msg = (
                        "metadata has missing fields "
                        + str(missing_fields)
                        + ". Its dataset folder will be ignored"
                    )
                    new_out_man.add_issues_entry_to_dict(
                        metadata_fp, pc.PROCESS_PROSPECTOR, msg
                    )
                    new_out_man.add_file_to_ignore(metadata_fp)
                    metadata_fp_dir = os.path.dirname(metadata_fp)
                    new_out_man.add_dir_to_ignore(metadata_fp_dir)
                    logger.warning("In " + metadata_fp + ", " + msg)

        return new_out_man

    def _search_for_missing_fields(
        self,
        md_dict: dict[str, Any],
    ) -> list[str]:
        """
        Search for missing fields in a metadata dictionary.

        Args:
            md_dict (dict[str, Any]): A dictionary containing the metadata.

        Returns:
            list[str]: A list of missing fields.
        """
        missing_fields: List[str] = []

        sub_dict_fields_to_check: List[str] = [
            amc.PROJECT_FIELD,
            amc.SAMPLE_FIELD,
            amc.SEQUENCE_FIELD,
        ]

        fields_to_check: List[str] = [amc.DESCRIPTION_FIELD, amc.INSTRUMENT_FIELD]

        if amc.CONFIG_FIELD in md_dict:
            md_dict = md_dict[amc.CONFIG_FIELD]

        if amc.BASENAME_FIELD in md_dict:
            sub_dict = md_dict[amc.BASENAME_FIELD]
            if amc.PROJECT_FIELD not in sub_dict:
                missing_fields.append(amc.PROJECT_FIELD)
            if amc.SAMPLE_FIELD not in sub_dict:
                missing_fields.append(amc.SAMPLE_FIELD)
            if amc.SEQUENCE_FIELD not in sub_dict:
                missing_fields.append(amc.SEQUENCE_FIELD)
        else:
            missing_fields.append(amc.BASENAME_FIELD)

        for field_to_check in fields_to_check:
            if field_to_check not in md_dict:
                missing_fields.append(field_to_check)

        if amc.INSTRUMENT_FIELD not in md_dict:
            missing_fields.append(amc.INSTRUMENT_FIELD)

        return missing_fields
