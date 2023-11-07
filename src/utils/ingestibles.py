# pylint: disable-all
"""A model-like class that is designed to contain the raw dataclasses for
refinery/ingestion. The raw dataclasses are stored in lists.
"""


# ---Imports
import logging

from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject

# from typing import List


# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class IngestibleDataclasses:
    """
    Class to manage and ingestible data classes.

    It provides methods to add and get projects, experiments, datasets and data files.

    Attributes:
        projects (list[RawProject]): List of projects.
        experiments (list[RawExperiment]): List of experiments.
        datasets (list[RawDataset]): List of datasets.
        datafiles (list[RawDatafile]): List of data files.
    """

    def __init__(
        self,
    ) -> None:
        self._setup()

    def _setup(
        self,
    ) -> None:
        self.projects: list[RawProject] = []
        self.experiments: list[RawExperiment] = []
        self.datasets: list[RawDataset] = []
        self.datafiles: list[RawDatafile] = []

    def get_projects(
        self,
    ) -> list[RawProject]:
        return self.projects

    def get_experiments(
        self,
    ) -> list[RawExperiment]:
        return self.experiments

    def get_datasets(
        self,
    ) -> list[RawDataset]:
        return self.datasets

    def get_datafiles(
        self,
    ) -> list[RawDatafile]:
        return self.datafiles

    def add_project(self, project: RawProject) -> None:
        self.projects.append(project)

    def add_experiment(self, experiment: RawExperiment) -> None:
        self.experiments.append(experiment)

    def add_dataset(self, dataset: RawDataset) -> None:
        self.datasets.append(dataset)

    def add_datafile(self, datafile: RawDatafile) -> None:
        self.datafiles.append(datafile)

    def add_projects(self, projects: list[RawProject]) -> None:
        self.projects.extend(projects)

    def add_experiments(self, experiments: list[RawExperiment]) -> None:
        self.experiments.extend(experiments)

    def add_datasets(self, datasets: list[RawDataset]) -> None:
        self.datasets.extend(datasets)

    def add_datafiles(self, datafiles: list[RawDatafile]) -> None:
        self.datafiles.extend(datafiles)
