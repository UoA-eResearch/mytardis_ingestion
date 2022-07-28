# pylint: disable=duplicate-code,logging-fstring-interpolation
"""IngestionFactory is a base class for specific instances of MyTardis
Ingestion scripts. The base class contains mostly concrete functions but
needs to determine the Smelter class that is used by the Factory"""

import logging
from typing import List, Optional, Tuple

from src.blueprints.custom_data_types import URI
from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.crucible.crucible import Crucible
from src.forges import Forge
from src.helpers.dataclass import get_object_name
from src.overseers import Inspector, Overseer
from src.smelters import Smelter

logger = logging.getLogger(__name__)


class IngestionFactory:
    """Ingestion Factory base class to orchestrate the Smelting, and Forging of
    objects within MyTardis.

    IngestionFactory is an abstract base class from which specific ingestion
    factories should be subclassed. The Factory classes orchestrate the various
    classes associated with ingestion in such a way that the Smelter, Overseer and Forge
    classes are unaware of each other

    Attributes:
        self.overseer: An instance of the Overseer class
        self.mytardis_setup: The return from the introspection API that specifies how MyTardis is
            set up.
        self.forge: An instance of the Forge class
        self.smelter: An instance of a smelter class that varies for different ingestion approaches
        self.glob_string: For smelters that use files, what is the extension or similar to search
            for. See pathlib documentations for glob details.
        self.default_institution: Either a name, or an identifier for an Institution to use as the
            default for Project and Experiment creation.
    """

    def __init__(
        self,
        # mytardis_setup: IntrospectionConfig,
        smelter: Smelter,
        inspector: Inspector,
        forge: Forge,
        overseer: Overseer,
        crucible: Crucible,
    ) -> None:
        """Initialises the Factory with the configuration found in the config_dict.

        Passes the config_dict to the overseer and forge instances to ensure that the
        specific MyTardis configuration is shared across all classes

        Args:
            general : GeneralConfig
                Pydantic config class containing general information
            auth : AuthConfig
                Pydantic config class containing information about authenticating with a MyTardis
                instance
            connection : ConnectionConfig
                Pydantic config class containing information about connecting to a MyTardis instance
            mytardis_setup : IntrospectionConfig
                Pydantic config class containing information from the introspection API
            smelter : Smelter
                class instance of Smelter
        """
        self.overseer = overseer
        # self.mytardis_setup = mytardis_setup
        self.forge = forge
        self.smelter = smelter
        self.inspector = inspector
        self.crucible = crucible

    def process_projects(
        self,
        projects: List[RawProject],
    ) -> List[Tuple[str, Optional[URI]]]:
        """Wrapper function to create the projects from input files"""
        processed_list: list[tuple[str, URI | None]] = []
        for project in projects:
            name = get_object_name(project)
            if not name:
                logger.warning(
                    (
                        "Unable to find the name of the project, skipping "
                        f"project defined by {project}"
                    )
                )
                continue
            is_present = self.inspector.match_or_block_object(
                project
            )  # better name ("is" implies a bool)
            if is_present:
                processed_list.append((name, is_present))
                continue
            if self.inspector.is_blocked(project):
                logger.warning(
                    (
                        "Project is blocked for ingestion and can't be "
                        f"processed. Object is {project}"
                    )
                )
                processed_list.append((name, None))
                continue
            raw_project = self.smelter.smelt_project(project)
            if not raw_project:
                processed_list.append((name, None))
                self.inspector.block_object(project)
                continue
            refined_parameters = None
            if len(raw_project) == 2:
                refined_parameters = raw_project[1]
            refined_project = self.crucible.refine_project(raw_project[0])
            if not refined_project:
                processed_list.append((name, None))
                self.inspector.block_object(project)
                continue
            wrought_project = self.forge.forge_project(
                refined_project, refined_parameters
            )
            if isinstance(wrought_project[0], URI):
                processed_list.append((name, wrought_project[0]))
                continue
            processed_list.append((name, None))
            self.inspector.block_object(project)
        return processed_list

    def process_experiments(  # pylint: disable=too-many-locals
        self,
        experiments: List[RawExperiment],
    ) -> List[Tuple[str, Optional[URI]]]:
        """Wrapper function to create the experiments from input files"""
        processed_list: list[tuple[str, URI | None]] = []
        for experiment in experiments:
            name = get_object_name(experiment)
            if not name:
                logger.warning(
                    (
                        "Unable to find the name of the experiment, skipping "
                        f"experiment defined by {experiment}"
                    )
                )
                continue
            if self.inspector.is_blocked_by_parents(experiment):
                logger.warning(
                    (
                        "Experiment is blocked by it's parent for ingestion and "
                        f"can't be processed. Object is {experiment}"
                    )
                )
                processed_list.append((name, None))
                continue
            is_present = self.inspector.match_or_block_object(
                experiment
            )  # better name (see above)
            if is_present:
                processed_list.append((name, is_present))
                continue
            if self.inspector.is_blocked(experiment):
                logger.warning(
                    (
                        "Experiment is blocked for ingestion and can't be "
                        f"processed. Object is {experiment}"
                    )
                )
                processed_list.append((name, None))
                continue
            raw_experiment = self.smelter.smelt_experiment(experiment)
            if not raw_experiment:
                self.inspector.block_object(experiment)
                processed_list.append((name, None))
                continue
            refined_parameters = None
            if len(raw_experiment) == 2:
                refined_parameters = raw_experiment[1]
            refined_experiment = self.crucible.refine_experiment(raw_experiment[0])
            if not refined_experiment:
                processed_list.append((name, None))
                self.inspector.block_object(experiment)
                continue
            wrought_experiment = self.forge.forge_experiment(
                refined_experiment, refined_parameters
            )
            if isinstance(wrought_experiment[0], URI):
                processed_list.append((name, wrought_experiment[0]))
                continue
            processed_list.append((name, None))
            self.inspector.block_object(experiment)
        return processed_list

    def process_datasets(  # pylint: disable=too-many-locals
        self,
        datasets: List[RawDataset],
    ) -> List[Tuple[str, Optional[URI]]]:
        """Wrapper function to create the experiments from input files"""
        processed_list: list[tuple[str, URI | None]] = []
        for dataset in datasets:
            name = get_object_name(dataset)
            if not name:
                logger.warning(
                    (
                        "Unable to find the name of the dataset, skipping "
                        f"dataset defined by {dataset}"
                    )
                )
                continue
            if self.inspector.is_blocked_by_parents(dataset):
                logger.warning(
                    (
                        "Dataset is blocked by it's parent for ingestion and "
                        f"can't be processed. Object is {dataset}"
                    )
                )
                processed_list.append((name, None))
                continue
            is_present = self.inspector.match_or_block_object(dataset)
            if is_present:
                processed_list.append((name, is_present))
                continue
            if self.inspector.is_blocked(dataset):
                logger.warning(
                    (
                        "Experiment is blocked for ingestion and can't be "
                        f"processed. Object is {dataset}"
                    )
                )
                processed_list.append((name, None))
                continue
            raw_dataset = self.smelter.smelt_dataset(dataset)
            if not raw_dataset:
                self.inspector.block_object(dataset)
                processed_list.append((name, None))
                continue
            refined_parameters = None
            if len(raw_dataset) == 2:
                refined_parameters = raw_dataset[1]
            refined_dataset = self.crucible.refine_dataset(raw_dataset[0])
            if not refined_dataset:
                processed_list.append((name, None))
                self.inspector.block_object(dataset)
                continue
            wrought_dataset = self.forge.forge_dataset(
                refined_dataset, refined_parameters
            )
            if isinstance(wrought_dataset[0], URI):
                processed_list.append((name, wrought_dataset[0]))
                continue
            processed_list.append((name, None))
            self.inspector.block_object(dataset)
        return processed_list

    def process_datafiles(
        self,
        datafiles: List[RawDatafile],
    ) -> List[Tuple[str, Optional[URI]]]:
        """Wrapper function to create the experiments from input files"""
        processed_list: list[tuple[str, URI | None]] = []
        for datafile in datafiles:
            name = get_object_name(datafile)
            if not name:
                logger.warning(
                    (
                        "Unable to find the name of the datafile, skipping "
                        f"datafile defined by {datafile}"
                    )
                )
                continue
            if self.inspector.is_blocked_by_parents(datafile):
                logger.warning(
                    (
                        "Datafile is blocked by it's parent for ingestion and "
                        f"can't be processed. Object is {datafile}"
                    )
                )
                processed_list.append((name, None))
                continue
            is_present = self.inspector.match_or_block_object(datafile)
            if is_present:
                processed_list.append((name, is_present))
                continue
            if self.inspector.is_blocked(datafile):
                logger.warning(
                    (
                        "Experiment is blocked for ingestion and can't be "
                        f"processed. Object is {datafile}"
                    )
                )
                processed_list.append((name, None))
                continue
            raw_datafile = self.smelter.smelt_datafile(datafile)
            if not raw_datafile:
                self.inspector.block_object(datafile)
                processed_list.append((name, None))
                continue
            refined_datafile = self.crucible.refine_datafile(raw_datafile)
            if not refined_datafile:
                processed_list.append((name, None))
                self.inspector.block_object(datafile)
                continue
            wrought_datafile = self.forge.forge_datafile(refined_datafile)
            if isinstance(wrought_datafile, URI):
                processed_list.append((name, wrought_datafile))
                continue
            processed_list.append((name, None))
            self.inspector.block_object(datafile)
        return processed_list
