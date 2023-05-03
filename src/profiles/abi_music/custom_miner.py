"""Defines the methodology to convert the source metadata to a beneficiable
format on a path.
"""


# ---Imports
import copy
import json
import logging
import os
import yaml

from src.profiles import output_manager as om
from src.profiles import profile_consts as pc
from src.profiles import profile_helpers as ph
from src.profiles.abi_music import abi_music_consts as amc


# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class CustomMiner():
    """Profile-specific miner class

    Each profile has a custom miner class whose behaviour is based on the
    requirements of the researcher
    """
    def __init__(self,
             ) -> None:
        """Do not modify this method
        """
        pass

    def mine(self,
             path: str,
             recursive: bool = True,
             out_man: om.OutputManager = None,
             options: dict = None,
             ) -> dict:
        """Mines metadata in a path

        Args:
            path (str): the path to inspect for metadata
            recursive (bool): True to inspect all subdirectories in path, False to inspect path only
            out_man (om.OutputManager): class which stores info of outputs of the pre-ingestion processes
            options (dict): extra options for the inspection

        Returns:
            om.OutputManager: output manager instance containing the outputs of the process
        """
        if not out_man:
            out_man = om.OutputManager()
        else:
            out_man = copy.deepcopy(out_man)
        #Write the main inspection implementation here
        base_yaml_pth = os.path.join(".", "src", "profiles", "abi_music")
        proj_yaml_fp = os.path.join(base_yaml_pth, "project.yaml")
        expt_yaml_fp = os.path.join(base_yaml_pth, "experiment.yaml")
        dset_yaml_fp = os.path.join(base_yaml_pth, "dataset.yaml")
        dfile_yaml_fp = os.path.join(base_yaml_pth, "datafile.yaml")

        with open(proj_yaml_fp, 'r') as stream:
            proj_mappings = yaml.safe_load(stream)
        with open(expt_yaml_fp, 'r') as stream:
            expt_mappings = yaml.safe_load(stream)
        with open(dset_yaml_fp, 'r') as stream:
            dset_mappings = yaml.safe_load(stream)
        with open(dfile_yaml_fp, 'r') as stream:
            dfile_mappings = yaml.safe_load(stream)

        dclass_struct = self._identify_data_classes(path, out_man)
        out_man = self._mine_project_metadata(path, dclass_struct, proj_mappings, out_man)
        out_man = self._mine_experiment_metadata(path, dclass_struct, expt_mappings, out_man)
        out_man = self._mine_dataset_metadata(path, dclass_struct, dset_mappings, out_man)
        out_man = self._mine_datafile_metadata(path, dclass_struct, dfile_mappings, out_man)

        return out_man


    #Write rest of implementation here, use leading underscores for each method
    def _identify_data_classes(self,
                              path: str,
                              out_man: om.OutputManager,
                              recursive: bool = True,
                              ) -> dict:
        dclass_struct = {}
        new_out_man = copy.deepcopy(out_man)

        file_rej_list = new_out_man.files_to_ignore
        rel_file_rej_list = [os.path.relpath(x, path) for x in file_rej_list]
        rel_file_rej_lut = dict.fromkeys(rel_file_rej_list)

        dir_rej_list = new_out_man.dirs_to_ignore
        rel_dir_rej_list = [os.path.relpath(x, path) for x in dir_rej_list]
        rel_dir_rej_lut = dict.fromkeys(rel_dir_rej_list)

        for root, dirs, files in os.walk(path):
            rel_path = os.path.relpath(root, path)

            if rel_path not in rel_dir_rej_lut and rel_path != ".":
                path_components = rel_path.split(os.sep)

                dir_levels = len(path_components)
                if dir_levels < 4:
                    for i in range(dir_levels):
                        if i == 0:
                            if path_components[i] not in dclass_struct:
                                dclass_struct[path_components[i]] = {}
                        elif i == 1:
                            if path_components[i-1] not in dclass_struct:
                                dclass_struct[path_components[i-1]] = {}
                            if path_components[i] not in dclass_struct[path_components[i-1]]:
                                dclass_struct[path_components[i-1]][path_components[i]] = {}
                        elif i == 2:
                            if path_components[i-2] not in dclass_struct:
                                dclass_struct[path_components[i-2]] = {}
                            if path_components[i-1] not in dclass_struct[path_components[i-2]]:
                                dclass_struct[path_components[i-2]][path_components[i-1]] = {}
                            if path_components[i] not in dclass_struct[path_components[i-2]][path_components[i-1]]:
                                dclass_struct[path_components[i-2]][path_components[i-1]][path_components[i]] = {}

        return dclass_struct


    def _write_metadata_file(self,
                             fp: str,
                             metadata: dict,
                             ) -> None:
        with open(fp, 'w') as outfile:
            json.dump(metadata, outfile, ensure_ascii=False)


    def _mine_project_metadata(self,
                               path: str,
                               dclass_struct: dict,
                               mappings: dict,
                               out_man: om.OutputManager,
                               ) -> om.OutputManager:
        new_out_man = copy.deepcopy(out_man)
        for proj_key in dclass_struct.keys():
            metadata = self._generate_project_metadata(proj_key, mappings)
            fp = os.path.join(path, proj_key + pc.METADATA_FILE_SUFFIX + amc.METADATA_FILE_TYPE)
            self._write_metadata_file(fp, metadata)
            new_out_man.add_success_entry_to_dict(fp, pc.PROCESS_MINER, "project metadata file written")
        return new_out_man


    def _generate_project_metadata(self,
                                   proj_key: str,
                                   mappings: dict,
                                   ) -> dict:
        metadata = {}
        req_keys = [key for key in mappings.keys() if mappings[key][pc.REQUIRED_KEY]]
        for req_key in req_keys:
            if mappings[req_key][pc.USEDEFAULT_KEY]:
                metadata[mappings[req_key][pc.NAME_KEY]] = mappings[req_key][pc.DEFAULT_KEY]
        metadata["principal_investigator"] = "gsan005"
        metadata["name"] = proj_key
        metadata["description"] = proj_key

        return metadata


    def _mine_experiment_metadata(self,
                                  path: str,
                                  dclass_struct: dict,
                                  mappings: dict,
                                  out_man: om.OutputManager,
                                  ) -> om.OutputManager:
        new_out_man = copy.deepcopy(out_man)
        for proj_key in dclass_struct.keys():
            for expt_key in dclass_struct[proj_key].keys():
                metadata = self._generate_experiment_metadata(proj_key, expt_key, mappings)
                fp = os.path.join(path, proj_key, expt_key + pc.METADATA_FILE_SUFFIX + amc.METADATA_FILE_TYPE)
                self._write_metadata_file(fp, metadata)
                new_out_man.add_success_entry_to_dict(fp, pc.PROCESS_MINER, "experiment metadata file written")

        return new_out_man


    def _generate_experiment_metadata(self,
                                      proj_key: str,
                                      expt_key: str,
                                      mappings: dict,
                                      ) -> dict:
        metadata = {}
        req_keys = [key for key in mappings.keys() if mappings[key][pc.REQUIRED_KEY]]
        for req_key in req_keys:
            if mappings[req_key][pc.USEDEFAULT_KEY]:
                metadata[mappings[req_key][pc.NAME_KEY]] = mappings[req_key][pc.DEFAULT_KEY]
        metadata["title"] = expt_key
        metadata["description"] = expt_key
        metadata["projects"] = str([proj_key])

        return metadata


    def _mine_dataset_metadata(self,
                               path: str,
                               dclass_struct: dict,
                               mappings: dict,
                               out_man: om.OutputManager,
                               ) -> om.OutputManager:
        new_out_man = copy.deepcopy(out_man)
        for proj_key in dclass_struct.keys():
            for expt_key in dclass_struct[proj_key].keys():
                for dset_key in dclass_struct[proj_key][expt_key].keys():
                    dset_metadata_fp = os.path.join(path, proj_key, expt_key, dset_key, dset_key + amc.METADATA_FILE_TYPE)
                    with open(dset_metadata_fp, 'r') as f:
                        dset_metadata = json.load(f)
                    config_key = "config"
                    if config_key in dset_metadata:
                        preproc_dset_metadata = {}
                        for con_key in dset_metadata[config_key].keys():
                            preproc_dset_metadata[con_key] = dset_metadata[config_key][con_key]
                        for key in dset_metadata.keys():
                            if key == config_key:
                                continue
                            preproc_dset_metadata[key] = dset_metadata[key]
                        dset_metadata = preproc_dset_metadata.copy()
                    flat_dset_metadata = self._flatten_dataset_dict(dset_metadata)
                    remapped_metadata = self._remap_dataset_fields(flat_dset_metadata, mappings)
                    fp = os.path.join(path, proj_key, expt_key, dset_key + pc.METADATA_FILE_SUFFIX + amc.METADATA_FILE_TYPE)
                    self._write_metadata_file(fp, remapped_metadata)
                    new_out_man.add_success_entry_to_dict(fp, pc.PROCESS_MINER, "dataset metadata file written")

        return new_out_man


    def _remap_dataset_fields(self,
                      flattened_dict: dict[str | int | float],
                      mappings: dict,
                      ) -> dict:
        remapped_dict = {}
        req_keys = [key for key in mappings.keys() if mappings[key][pc.REQUIRED_KEY]]
        other_keys = [key for key in mappings.keys() if not mappings[key][pc.REQUIRED_KEY]]
        for req_key in req_keys:
            if mappings[req_key][pc.USEDEFAULT_KEY]\
                    and req_key not in flattened_dict:
                remapped_dict[mappings[req_key][pc.NAME_KEY]] = mappings[req_key][pc.DEFAULT_KEY]
            else:
                remapped_dict[mappings[req_key][pc.NAME_KEY]] = flattened_dict[req_key]

        for key in other_keys:
            if mappings[key][pc.USEDEFAULT_KEY]\
                    and key not in flattened_dict:
                remapped_dict[mappings[key][pc.NAME_KEY]] = mappings[key][pc.DEFAULT_KEY]

        for key in flattened_dict.keys():
            if key not in remapped_dict and key not in mappings:
                remapped_dict[key] = flattened_dict[key]

        remapped_dict["identifiers"] = remapped_dict.pop("SequenceID")
        remapped_dict["identifiers"] = [remapped_dict["identifiers"]]
        remapped_dict["experiments"] = [remapped_dict["experiments"]]
        remapped_dict["summary"] = flattened_dict["Description"]

        dict_keys = list(remapped_dict.keys())
        dict_keys.sort()
        remapped_dict = {i: remapped_dict[i] for i in dict_keys}

        return remapped_dict


    def _flatten_dataset_dict(self,
                      d: dict,
                      ) -> dict:
        flat_dset_metadata = {}
        for key, value in d.items():
            if isinstance(value, dict):
                nested = self._flatten_dataset_dict(value)
                for nested_key, nested_value in nested.items():
                    frmtd_key = key + pc.KEY_LVL_SEP + nested_key
                    flat_dset_metadata[frmtd_key] = nested_value
            elif isinstance(value, list):
                for idx, elem in enumerate(value):
                    if isinstance(elem, dict):
                        nested = self._flatten_dataset_dict(elem)
                        for nested_key, nested_value in nested.items():
                            frmtd_key = key + pc.KEY_IDX_OP + str(idx) + pc.KEY_IDX_CL + pc.KEY_LVL_SEP + nested_key
                            flat_dset_metadata[frmtd_key] = nested_value
                    else:
                        frmtd_key = key + pc.KEY_IDX_OP + str(idx) + pc.KEY_IDX_CL
                        flat_dset_metadata[frmtd_key] = elem
            else:
                flat_dset_metadata[key] = value

        return flat_dset_metadata


    def _mine_datafile_metadata(self,
                                path: str,
                                dclass_struct: dict,
                                mappings: dict,
                                out_man: om.OutputManager,
                                ) -> om.OutputManager:
        new_out_man = copy.deepcopy(out_man)
        files_to_ignore = new_out_man.files_to_ignore
        files_to_ignore_lut = dict.fromkeys(files_to_ignore)

        for proj_key in dclass_struct.keys():
            for expt_key in dclass_struct[proj_key].keys():
                for dset_key in dclass_struct[proj_key][expt_key].keys():
                    base_path = os.path.join(path, proj_key, expt_key, dset_key)
                    logger.info("mining files in: {0}".format(base_path))
                    for root, dirs, files in os.walk(base_path):
                        rel_path = os.path.relpath(root, base_path)
                        for file in files:
                            if os.path.join(root,file) in files_to_ignore_lut:
                                continue
                            metadata = self._generate_datafile_metadata(root, rel_path, dset_key, file)
                            fp = os.path.join(root, file + pc.METADATA_FILE_SUFFIX + amc.METADATA_FILE_TYPE)
                            self._write_metadata_file(fp, metadata)
                            new_out_man.add_success_entry_to_dict(fp, pc.PROCESS_MINER, "dataset metadata file written")

        return new_out_man


    def _generate_datafile_metadata(self,
                                    root: str,
                                    rel_path: str,
                                    dset_key: str,
                                    fn: str,
                                    ) -> dict:
        metadata = {}
        fp = os.path.join(root, fn)

        metadata["dataset"] = dset_key
        metadata["filename"] = fn
        metadata["directory"] = rel_path
        metadata["md5sum"] = ph.calculate_md5sum(fp)
        metadata["mimetype"] = ph.determine_mimetype(fn)
        metadata["size"] = os.path.getsize(fp)

        return metadata


