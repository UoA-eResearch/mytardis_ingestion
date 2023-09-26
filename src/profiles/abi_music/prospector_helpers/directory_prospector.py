"""Prospects a directory to check whether the directory path can be ingested
"""

# ---Imports
import copy
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

from src.extraction_output_manager import output_manager as om
from src.profiles import profile_consts as pc
from src.profiles.abi_music import abi_music_consts as amc

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class DirectoryProspector:
    """Prospects a directory to check whether the directory path can be ingested"""

    def __init__(
        self,
    ) -> None:
        return

    def check_for_files_outside_dataset(
        self,
        path: Path,
        out_man: om.OutputManager,
    ) -> om.OutputManager:
        """Check for files outside dataset folders and store them in the output manager
        as well as log the files

        Args:
            path (Path): the path that is at least a level above the dataset
            out_man (om.OutputManager): the output manager to store information of the offending files

        Returns:
            om.OutputManager: the output manager to store information of the offending files
        """
        new_out_man = copy.deepcopy(out_man)
        for root, dirs, files in os.walk(path):
            root_pth = Path(root)
            rel_path = root_pth.relative_to(path)
            if len(rel_path.parts) < 3:
                for file in files:
                    if pc.METADATA_FILE_SUFFIX in file:
                        continue
                    fp = root_pth / Path(file)
                    new_out_man.add_issues_entry_to_dict(
                        fp, pc.PROCESS_PROSPECTOR, "not in a dataset folder"
                    )
                    new_out_man.add_file_to_ignore(fp)
                    logger.warning(
                        f"{file} file found in {rel_path} which is not in a dataset folder"
                    )
        return new_out_man

    def check_json_folder_path_mismatch(
        self,
        path: str,
        out_man: om.OutputManager,
    ) -> tuple[om.OutputManager, list[str]]:
        """
        Checks if a folder path corresponds to a json file with the same name.

        Args:
            path (str): The path to the folder.
            out_man (om.OutputManager): The OutputManager associated with the folder path.

        Returns:
            tuple[om.OutputManager, list[str]]: A tuple containing the OutputManager and a list of metadata filepaths.
        """
        metadata_fp_list = []
        new_out_man = copy.deepcopy(out_man)
        rej_list = new_out_man.files_to_ignore

        if rej_list:
            rel_rej_list = [os.path.relpath(x, path) for x in rej_list]
            rej_lut = dict.fromkeys(rel_rej_list)
        for root, dirs, files in os.walk(path):
            root_pth = Path(root)
            rel_path = root_pth.relative_to(path)

            if not root_pth.exists():
                continue
            elif rel_path.count(os.sep) != 2:
                continue

            # target_dir = os.path.normpath(rel_path).name
            json_sufx = ".json"
            target_dir = rel_path.resolve().name
            target_file = Path(target_dir + json_sufx)

            has_match = False
            matched_filepath = ""
            for file in files:
                if rej_list:
                    lookup = os.path.join(rel_path, file)
                    if lookup in rej_lut:
                        continue
                if ".json" in file:
                    if target_file == file:
                        has_match = True
                        matched_filepath = root_pth / Path(file)

            if has_match:
                if self._determine_json_matches_folder_path(matched_filepath, rel_path):
                    new_out_man.add_success_entry_to_dict(
                        matched_filepath,
                        pc.PROCESS_PROSPECTOR,
                        amc.OUTPUT_NOTE_JSON_MATCH_SUCCESS,
                    )
                    metadata_fp_list.append(matched_filepath)
                else:
                    new_out_man.add_issues_entry_to_dict(
                        matched_filepath,
                        pc.PROCESS_PROSPECTOR,
                        amc.OUTPUT_NOTE_JSON_MATCH_FAIL,
                    )
                    new_out_man.add_dir_to_ignore(root)
            else:
                logger.warning(
                    "no corresponding .json file found in {0}".format(rel_path)
                )
                new_out_man.add_dir_to_ignore(root)

        return new_out_man, metadata_fp_list

    def _determine_json_matches_folder_path(
        self,
        matched_fp: str,
        rel_path: str,
    ) -> bool:
        """Checks if the path contents inside the json metadata file matches
        those of the actual folder path.

        Args:
            matched_fp (str): filepath of the json metadata file
            rel_path (str): actual folder path of the json metadata file

        Returns:
            bool: True if matching, False otherwise
        """
        with open(matched_fp, "r") as f:
            metadata = json.load(f)

        basename_data = {}
        if amc.CONFIG_FIELD in metadata.keys():
            basename_data = metadata[amc.CONFIG_FIELD][amc.BASENAME_FIELD]
        else:
            basename_data = metadata[amc.BASENAME_FIELD]

        prj_name = basename_data[amc.PROJECT_FIELD]
        smp_name = basename_data[amc.SAMPLE_FIELD]
        seq_name = basename_data[amc.SEQUENCE_FIELD]

        ref_path = os.path.join(prj_name, smp_name, seq_name)
        if not ref_path in rel_path:
            return False

        dir_sufx = str(rel_path).split(str(ref_path))[1]
        if dir_sufx in amc.FOLDER_SUFFIX_LUT:
            return True
        else:
            return False
