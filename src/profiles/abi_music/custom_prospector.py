"""Defines the methodology to inspect metadata and perform related checks on a path.
"""


# ---Imports
import copy
import json
import logging
import os

from src.profiles import output_manager as om
from src.profiles import profile_consts as pc
from src.profiles.abi_music import abi_music_consts as amc


# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class CustomProspector():
    """Profile-specific prospector class

    Each profile has a custom prospector whose behaviour is based on the
    requirements of the researcher 
    """
    def __init__(self,
             ) -> None:
        """Do not modify this method
        """
        pass

    def inspect(self,
                path: str,
                recursive: bool,
                out_man: om.OutputManager = None,
                ) -> dict:
        """Prospects metadata in a path

        Args:
            path (str): the path to inspect for metadata
            recursive (bool): True to inspect all subdirectories in path, False to inspect path only
            out_man (om.OutputManager): class which stores info of outputs of the pre-ingestion processes

        Returns:
            om.OutputManager: output manager instance containing the outputs of the process
        """
        if not out_man:
            out_man = om.OutputManager()

        #Write the main inspection implementation here
        logger.info("Checking for corresponding raw-processed folder pairs")
        self._check_for_raw_and_processed_folder_pairs(path)
        logger.info("Checking for whether .json metadata file matches the file's path prescribed in its metadata")
        out_man, metadata_fp_list = self._check_json_folder_path_mismatch(path, out_man)
        logger.info("Checking files outside dataset folders")
        out_man = self._check_for_files_outside_dataset(path, out_man)
        logger.info("Checking metadata files for their required fields")
        out_man = self._check_metadata_for_mining(out_man, metadata_fp_list)

        return out_man


    def _check_json_folder_path_mismatch(self,
                                         path: str,
                                         out_man: om.OutputManager,
                                         ) -> (om.OutputManager, list[str]):
        metadata_fp_list = []
        new_out_man = copy.deepcopy(out_man)
        rej_list = new_out_man.files_to_ignore

        if rej_list:
            rel_rej_list = [os.path.relpath(x, path) for x in rej_list]
            rej_lut = dict.fromkeys(rel_rej_list)
        for root, dirs, files in os.walk(path):
            rel_path = os.path.relpath(root, path)
            if rel_path == "":
                continue
            elif rel_path.count(os.sep) != 2:
                continue

            target_dir = os.path.basename(os.path.normpath(rel_path))
            target_file = target_dir + ".json"
            has_match = False
            matched_filepath = ""
            for file in files:
                if rej_list:
                    lookup = os.path.join(rel_path,file)
                    if lookup in rej_lut:
                        continue
                if ".json" in file:
                    if target_file == file:
                        has_match = True
                        matched_filepath = os.path.join(root, file)

            if has_match:
                json_matches_folder_path = self._determine_json_matches_folder_path(matched_filepath, rel_path)
                if json_matches_folder_path:
                    new_out_man.add_success_entry_to_dict(matched_filepath,
                                                          pc.PROCESS_PROSPECTOR,
                                                          amc.OUTPUT_NOTE_JSON_MATCH_SUCCESS)
                    metadata_fp_list.append(matched_filepath)
                else:
                    new_out_man.add_issues_entry_to_dict(matched_filepath,
                                                         pc.PROCESS_PROSPECTOR,
                                                         amc.OUTPUT_NOTE_JSON_MATCH_FAIL)
                    new_out_man.add_dir_to_ignore(root)
            else:
                logger.warning("no corresponding .json file found in {0}".format(rel_path))
                new_out_man.add_dir_to_ignore(root)

        return new_out_man, metadata_fp_list


    def _determine_json_matches_folder_path(
            self,
            matched_fp: str,
            rel_path: str,
            ) -> bool:
        with open(matched_fp, 'r') as f:
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
        dir_sufx = rel_path.split(ref_path)[1]
        if dir_sufx in amc.FOLDER_SUFFIX_LUT:
            return True
        else:
            return False


    def _check_for_raw_and_processed_folder_pairs(
            self,
            path: str,
            ) -> None:

        dsets = []
        for root, dirs, files in os.walk(path):
            rel_path = os.path.relpath(root, path)
            if rel_path == "":
                continue
            elif rel_path.count(os.sep) != 2:
                continue

            dsets.append(rel_path)

        dsets_lut = dict.fromkeys(dsets)
        used_lut = {}
        for item in dsets:
            dset_basename = item.rsplit("-")[0]
            if dset_basename in used_lut:
                if amc.RAW_FOLDER_SUFFIX in item:
                    used_lut[dset_basename] += 1
                elif amc.DECONV_FOLDER_SUFFIX in item:
                    used_lut[dset_basename] += 2
                continue
            dsets_lut = {k: v for k, v in dsets_lut.items() if not dset_basename in k}
            if amc.RAW_FOLDER_SUFFIX in item:
                used_lut[dset_basename] = 1
            elif amc.DECONV_FOLDER_SUFFIX in item:
                used_lut[dset_basename] = 2
            else:
                logger.warning("folder {0} does not have a conforming suffix. Should end in one of {1}".format(item, amc.FOLDER_SUFFIX_KEYS))

        for key in used_lut.keys():
            if used_lut[key] == 1:
                logger.warning("missing processed folder for {0}".format(key))
            elif used_lut[key] == 2:
                logger.warning("missing raw folder for {0}".format(key))


    def _check_for_files_outside_dataset(
            self,
            path: str,
            out_man: om.OutputManager,
            ) -> None:
        new_out_man = copy.deepcopy(out_man)
        for root, dirs, files in os.walk(path):
            rel_path = os.path.relpath(root, path)
            if rel_path.count(os.sep) < 2:
                for file in files:
                    if pc.METADATA_FILE_SUFFIX in file:
                        continue
                    fp = os.path.join(root, file)
                    new_out_man.add_issues_entry_to_dict(fp,
                                                         pc.PROCESS_PROSPECTOR,
                                                         "not in a dataset folder")
                    new_out_man.add_file_to_ignore(fp)
                    logger.warning("{0} file found in {1} which is not in a dataset folder".format(file, rel_path))
        return new_out_man


    def _check_metadata_for_mining(self,
                                   out_man: om.OutputManager,
                                   metadata_fp_list: list[str],
                                   ) -> om.OutputManager:
        new_out_man = copy.deepcopy(out_man)
        for metadata_fp in metadata_fp_list:
            with open(metadata_fp, 'r') as f:
                md_dict = json.load(f)
            missing_fields = self._search_for_missing_fields(md_dict)

            if len(missing_fields) == 0:
                new_out_man.add_success_entry_to_dict(metadata_fp,
                                                      pc.PROCESS_PROSPECTOR,
                                                      "metadata is ready for ingestion")
            else:
                is_still_ingestable = True
                for field in missing_fields:
                    if field not in amc.FIELDS_WITH_DEFAULTS_LUT:
                        is_still_ingestable = False
                        break

                if is_still_ingestable:
                    msg = "metadata is ready for ingestion, but will have default values assigned for missing fields"
                    new_out_man.add_success_entry_to_dict(metadata_fp,
                                                          pc.PROCESS_PROSPECTOR,
                                                          msg)
                else:
                    msg = "metadata has missing fields " + str(missing_fields) + ". Its dataset folder will be ignored"
                    new_out_man.add_issues_entry_to_dict(metadata_fp,
                                                         pc.PROCESS_PROSPECTOR,
                                                         msg)
                    new_out_man.add_file_to_ignore(metadata_fp)
                    metadata_fp_dir = os.path.dirname(metadata_fp)
                    new_out_man.add_dir_to_ignore(metadata_fp_dir)
                    logger.warning("In " + metadata_fp + ", " + msg)

        return new_out_man


    def _search_for_missing_fields(self,
                                 md_dict: dict,
                                 ) -> list:
        missing_fields = []

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

        if amc.DESCRIPTION_FIELD not in md_dict:
            missing_fields.append(amc.DESCRIPTION_FIELD)

        if amc.INSTRUMENT_FIELD not in md_dict:
            missing_fields.append(amc.INSTRUMENT_FIELD)

        return missing_fields