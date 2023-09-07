"""A model-like class that is designed to contain the raw dataclasses for
refinery/ingestion. The raw dataclasses are stored in lists.
"""


# ---Imports
import logging
from typing import List

from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class IngestibleDataclasses:
    """
    Class to manage and ingestible data classes.

    It provides methods to add and get projects, experiments, datasets and data files.

    Attributes:
        projects (List[RawProject]): List of projects.
        experiments (List[RawExperiment]): List of experiments.
        datasets (List[RawDataset]): List of datasets.
        datafiles (List[RawDatafile]): List of data files.
    """

    def __init__(
        self,
    ) -> None:
        self.projects: List[RawProject] = []
        self.experiments: List[RawExperiment] = []
        self.datasets: List[RawDataset] = []
        self.datafiles: List[RawDatafile] = []

    def get_projects(  # pylint: disable=missing-function-docstring
        self,
    ) -> List[RawProject]:
        return self.projects

    def get_experiments(  # pylint: disable=missing-function-docstring
        self,
    ) -> List[RawExperiment]:
        return self.experiments

    def get_datasets(  # pylint: disable=missing-function-docstring
        self,
    ) -> List[RawDataset]:
        return self.datasets

    def get_datafiles(  # pylint: disable=missing-function-docstring
        self,
    ) -> List[RawDatafile]:
        return self.datafiles

    def add_project(  # pylint: disable=missing-function-docstring
        self,
        project: RawProject,
    ) -> None:
        self.projects.append(project)

    def add_experiment(  # pylint: disable=missing-function-docstring
        self,
        experiment: RawExperiment,
    ) -> None:
        self.experiments.append(experiment)

    def add_dataset(  # pylint: disable=missing-function-docstring
        self,
        dataset: RawDataset,
    ) -> None:
        self.datasets.append(dataset)

    def add_datafile(  # pylint: disable=missing-function-docstring
        self,
        datafile: RawDatafile,
    ) -> None:
        self.datafiles.append(datafile)

    def add_projects(  # pylint: disable=missing-function-docstring
        self,
        projects: List[RawProject],
    ) -> None:
        self.projects.extend(projects)

    def add_experiments(  # pylint: disable=missing-function-docstring
        self,
        experiments: List[RawExperiment],
    ) -> None:
        self.experiments.extend(experiments)

    def add_datasets(  # pylint: disable=missing-function-docstring
        self,
        datasets: List[RawDataset],
    ) -> None:
        self.datasets.extend(datasets)

    def add_datafiles(  # pylint: disable=missing-function-docstring
        self,
        datafiles: List[RawDatafile],
    ) -> None:
        self.datafiles.extend(datafiles)
