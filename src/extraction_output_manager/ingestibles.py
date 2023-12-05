# pylint: disable-all
"""A model-like class that is designed to contain the raw dataclasses for
refinery/ingestion. The raw dataclasses are stored in lists.
"""

# ---Imports
from __future__ import annotations

import logging
from typing import List, Optional, Sequence, TextIO

from pydantic import BaseModel

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
        projects: Optional[list[RawProject]] = None,
        experiments: Optional[list[RawExperiment]] = None,
        datasets: Optional[list[RawDataset]] = None,
        datafiles: Optional[list[RawDatafile]] = None,
    ) -> None:
        self._projects = projects or []
        self._experiments = experiments or []
        self._datasets = datasets or []
        self._datafiles = datafiles or []

    def get_projects(  # pylint: disable=missing-function-docstring
        self,
    ) -> List[RawProject]:
        return self._projects

    def get_experiments(  # pylint: disable=missing-function-docstring
        self,
    ) -> List[RawExperiment]:
        return self._experiments

    def get_datasets(  # pylint: disable=missing-function-docstring
        self,
    ) -> List[RawDataset]:
        return self._datasets

    def get_datafiles(  # pylint: disable=missing-function-docstring
        self,
    ) -> List[RawDatafile]:
        return self._datafiles

    def add_project(  # pylint: disable=missing-function-docstring
        self,
        project: RawProject,
    ) -> None:
        self._projects.append(project)

    def add_experiment(  # pylint: disable=missing-function-docstring
        self,
        experiment: RawExperiment,
    ) -> None:
        self._experiments.append(experiment)

    def add_dataset(  # pylint: disable=missing-function-docstring
        self,
        dataset: RawDataset,
    ) -> None:
        self._datasets.append(dataset)

    def add_datafile(  # pylint: disable=missing-function-docstring
        self,
        datafile: RawDatafile,
    ) -> None:
        self._datafiles.append(datafile)

    def add_projects(  # pylint: disable=missing-function-docstring
        self,
        projects: List[RawProject],
    ) -> None:
        self._projects.extend(projects)

    def add_experiments(  # pylint: disable=missing-function-docstring
        self,
        experiments: List[RawExperiment],
    ) -> None:
        self._experiments.extend(experiments)

    def add_datasets(  # pylint: disable=missing-function-docstring
        self,
        datasets: List[RawDataset],
    ) -> None:
        self._datasets.extend(datasets)

    def add_datafiles(  # pylint: disable=missing-function-docstring
        self,
        datafiles: List[RawDatafile],
    ) -> None:
        self._datafiles.extend(datafiles)

    def print(self, stream: TextIO, skip_datafiles: bool = True) -> None:
        def write_header(text: str) -> None:
            stream.write(f"\n\n{'=' * len(text)}\n")
            stream.write(text)
            stream.write(f"\n{'=' * len(text)}\n\n")

        def write_dataclasses(models: Sequence[BaseModel]) -> None:
            for model in models:
                stream.write(model.model_dump_json(indent=4))
                stream.write("\n")

        write_header("Projects")
        write_dataclasses(self.get_projects())

        write_header("Experiments")
        write_dataclasses(self.get_experiments())

        write_header("Datasets")
        write_dataclasses(self.get_datasets())

        if not skip_datafiles:
            write_header("Datafiles")
            write_dataclasses(self.get_datafiles())
