"""
Generates a folder for a custom prospector/miner setup tailored for a specific profile

The reason for custom prospector/miner setups is that for non-IME-based metadata generation
different researchers will have different methods of metadata setup, thus a profile
is generated for the specific setup.
"""


# ---Imports
import logging
import shutil
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.profiles import profile_consts as pc

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class ProfileGenerator:
    """Used to generate a module of custom prospectors and miners that are
    specific to the researcher/instrument.
    """

    def __init__(
        self,
    ) -> None:
        base_pth = Path("src")
        prof_sub_pth = Path("profiles")
        tmpl_sub_pth = Path("template")
        self.profile_path = base_pth / prof_sub_pth
        self.template_path = self.profile_path / tmpl_sub_pth

    def generate_profile(
        self,
        profile_name: str,
        include_configenv: bool,
    ) -> None:
        """Autogenerate a profile folder
        A profile folder has the necessary components required
        to prospect and mine a directory for a given

        Args:
            profile_name (str): name of the profile that will dictate the folder name
            include_configenv (bool): generates a config.env file if true

        Raises:
            Exception: raises exception if profile already exists
        """
        prof_pth = self.profile_path / Path(profile_name)
        if prof_pth.exists():
            logger.error(f"Profile with name {profile_name} already exists.")
            raise Exception(f"Profile with name {profile_name} already exists.")

        dst = self._create_profile_folder(
            self.profile_path, profile_name, include_configenv
        )
        self._generate_key_luts(dst)

    def _create_profile_folder(
        self,
        profile_pth: Path,
        profile_name: str,
        include_configenv: bool,
    ) -> Path:
        """Create the profile folder by copying the template folder

        Args:
            profile_pth (Path): path to the profile
            profile_name (str): name of the profile
            include_configenv (bool): whether to include the config.env file in the profile

        Returns:
            Path: path to the copied folder
        """
        src = self.template_path
        prof_name_pth = Path(profile_name)
        dst = profile_pth / prof_name_pth
        if include_configenv:
            shutil.copytree(src, dst)
        else:
            shutil.copytree(src, dst, ignore=shutil.ignore_patterns("*.env"))
        return dst

    def _generate_key_luts(
        self,
        base_pth: Path,
    ) -> None:
        """Generates a file for each dataclass that is used to setup the mapping
         between the instrument's fields to MyTardis's fields

        Args:
            base_pth (Path): path of the profile
        """
        raw_datafile_dict = self._generate_raw_dataclass_dict(RawDatafile)
        raw_dataset_dict = self._generate_raw_dataclass_dict(RawDataset)
        raw_experiment_dict = self._generate_raw_dataclass_dict(RawExperiment)
        raw_project_dict = self._generate_raw_dataclass_dict(RawProject)

        self._write_to_yaml_file(base_pth, pc.DATAFILE_NAME, raw_datafile_dict)
        self._write_to_yaml_file(base_pth, pc.DATASET_NAME, raw_dataset_dict)
        self._write_to_yaml_file(base_pth, pc.EXPERIMENT_NAME, raw_experiment_dict)
        self._write_to_yaml_file(base_pth, pc.PROJECT_NAME, raw_project_dict)

    def _generate_raw_dataclass_dict(
        self,
        class_type: RawDatafile | RawDataset | RawExperiment | RawProject,
    ) -> Dict[str, Dict[str, Any]]:
        """Generates a dict that is used to help map the fields of the rawdataclasses. The keys of this dict are the
        attributes of the raw dataclasses which are to be replaced by the equivalent keys from the instrument,
        and the values for each key is a dict that contains metadata on how to map the key.

        Args:
            class_type (RawDatafile | RawDataset | RawExperiment | RawProject): the dataclass to produce a dict for

        Returns:
            Dict[str,Any]: A dictionary containing the raw dataclasses's fields as the keys to a dictionary of
        """
        raw_dataclass_dict = {}
        fields_dict = class_type.__dict__["model_fields"]

        for key in fields_dict.keys():
            field_entry = fields_dict[key]
            entry_dict = dict.fromkeys(pc.DATACLASS_ENTRY_DICT_KEYS)
            entry_dict[pc.DEFAULT_KEY] = ""
            entry_dict[pc.NAME_KEY] = key
            entry_dict[pc.NATIVE_KEY] = True
            entry_dict[pc.REQUIRED_KEY] = field_entry.is_required()
            entry_dict[pc.USEDEFAULT_KEY] = False

            raw_dataclass_dict[key] = entry_dict

        return raw_dataclass_dict

    def _write_to_yaml_file(
        self,
        base_pth: Path,
        dataclass_name: str,
        data_dict: dict,
    ) -> None:
        dataclass_filename = dataclass_name + pc.DATACLASS_PROF_SUFFIX
        dclass_fn_pth = Path(dataclass_filename)
        fp = base_pth / dclass_fn_pth
        with fp.open("w") as f:
            yaml.dump(data_dict, f)


def standalone_generation():
    profile_name = input("Please enter profile name: ")
    y_or_n = input("Do you wish to generate a .env file with this profile? (y/n)")
    gen_env = False
    if y_or_n.lower() == "y":
        gen_env = True
    elif y_or_n.lower() == "n":
        gen_env = False
    else:
        raise Exception(
            "Invalid response (can only be either 'y' or 'n'), please try again"
        )
    p_gen = ProfileGenerator()
    p_gen.generate_profile(profile_name, gen_env)


if __name__ == "__main__":
    standalone_generation()
