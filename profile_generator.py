"""
Generates a folder for a custom prospector/miner setup tailored for a specific profile

The reason for custom prospector/miner setups is that for non-IME-based metadata generation
different researchers will have different methods of metadata setup, thus a profile 
is generated for the specific setup.
"""


# ---Imports
import logging
import os
import shutil

import yaml
from pydantic.fields import ModelField

from src.blueprints.common_models import Parameter
from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.helpers import GeneralConfig, SchemaConfig, log_if_projects_disabled
from src.helpers.config import StorageConfig
from src.overseers.overseer import Overseer
from src.profiles import profile_consts as pc

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class ProfileGenerator:


    def __init__(
        self,
    ) -> None:
        self.profile_path = os.path.join("./", "src", "profiles")
        self.template_path = os.path.join(self.profile_path, "template")


    def generate_profile(
        self,
        profile_name: str,
        include_configenv: bool,
    ) -> None:
        """Autogenerate a profile folder
        A profile folder has the necessary components required
        to prospect a directory

        Args:
            profile_name (str): name of the profile that will dictate the folder name
            include_configenv (bool): generates a config.env file if true

        Raises:
            Exception: raises exception if profile already exists
        """
        if os.path.exists(profile_name):
            logger.error("Profile with name {0} already exists.".format(profile_name))
            raise Exception(
                "Profile with name {0} already exists.".format(profile_name)
            )

        dst = self._create_profile_folder(self.profile_path, profile_name, include_configenv)
        self._generate_key_luts(dst)


    def _create_profile_folder(
        self,
        profile_pth: str,
        profile_name: str,
        include_configenv: bool,
    ) -> str:
        src = self.template_path
        dst = os.path.join(profile_pth, profile_name)
        if include_configenv:
            shutil.copytree(src, dst)
        else:
            shutil.copytree(src, dst, ignore=shutil.ignore_patterns("*.env"))
        return dst


    def _generate_key_luts(
        self,
        base_pth: str,
    ) -> None:
        raw_datafile_dict = self._generate_raw_dataclass_dict(RawDatafile)
        raw_dataset_dict = self._generate_raw_dataclass_dict(RawDataset)
        raw_experiment_dict = self._generate_raw_dataclass_dict(RawExperiment)
        raw_project_dict = self._generate_raw_dataclass_dict(RawProject)
        
        self._write_to_yaml_file(base_pth, pc.DATAFILE_NAME, raw_datafile_dict)
        self._write_to_yaml_file(base_pth, pc.DATASET_NAME, raw_dataset_dict)
        self._write_to_yaml_file(base_pth, pc.EXPERIMENT_NAME, raw_experiment_dict)
        self._write_to_yaml_file(base_pth, pc.PROJECT_NAME, raw_project_dict)
        

    def _generate_raw_dataclass_dict(self,
                                     class_type: RawDatafile|RawDataset|RawExperiment|RawProject,
                                    ) -> dict:
        raw_dataclass_dict = {}
        fields_dict = class_type.__dict__['__fields__']
        for key in fields_dict.keys():
            field_entry = fields_dict[key]
            entry_dict = dict.fromkeys(pc.DATACLASS_ENTRY_DICT_KEYS)
            entry_dict[pc.DEFAULT_KEY] = ""
            entry_dict[pc.NAME_KEY] = field_entry.name
            entry_dict[pc.NATIVE_KEY] = True
            entry_dict[pc.REQUIRED_KEY] = field_entry.required
            entry_dict[pc.USEDEFAULT_KEY] = False
            
            raw_dataclass_dict[field_entry.name] = entry_dict

        return raw_dataclass_dict
    

    def _write_to_yaml_file(self,
                       base_pth: str,
                       dataclass_name: str,
                       data_dict: dict,
                       ) -> None:
        dataclass_filename = dataclass_name + pc.DATACLASS_PROF_SUFFIX
        fp = os.path.join(base_pth, dataclass_filename)
        with open(fp, 'w') as f:
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
