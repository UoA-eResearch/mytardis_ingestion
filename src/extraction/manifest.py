# pylint: disable-all
"""A model-like class that is designed to contain the raw dataclasses for
refinery/ingestion. The raw dataclasses are stored in lists.
"""

# ---Imports
from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import List, Optional, Sequence, TextIO, Type, TypeVar

from pydantic import BaseModel

from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.utils.filesystem.filesystem_nodes import DirectoryNode

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


ModelT = TypeVar("ModelT", bound=BaseModel)


# ---Code
class IngestionManifest:
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

    def serialize(self, root_dir: Path) -> None:
        root_dir.mkdir(parents=True, exist_ok=True)

        def serialize_objects(objects: Sequence[BaseModel], dir_name: str) -> None:
            objects_dir = root_dir / dir_name
            objects_dir.mkdir()

            for i, obj in enumerate(objects):
                file_path = (objects_dir / str(i)).with_suffix(".json")
                with file_path.open("w", encoding="utf-8") as f:
                    f.write(obj.model_dump_json(by_alias=True))

        serialize_objects(self.get_projects(), "projects")
        serialize_objects(self.get_experiments(), "experiments")
        serialize_objects(self.get_datasets(), "datasets")
        serialize_objects(self.get_datafiles(), "datafiles")

    @staticmethod
    def deserialize(root_dir: Path) -> IngestionManifest:
        try:
            directory = DirectoryNode(root_dir)
        except NotADirectoryError as e:
            e.strerror = f"Failed to deserialize ingestion manifest: {e.strerror}"
            raise e

        def deserialize_objects(
            obj_dir: DirectoryNode, object_type: Type[ModelT]
        ) -> list[ModelT]:
            objects = []

            json_files = obj_dir.find_files(lambda p: p.extension() == ".json")

            for file in json_files:
                with file.path().open("r", encoding="utf-8") as f:
                    json_data = f.read()
                    obj = object_type.model_validate_json(json_data)
                    objects.append(obj)

            return objects

        projects = deserialize_objects(directory.dir("projects"), RawProject)
        experiments = deserialize_objects(directory.dir("experiments"), RawExperiment)
        datasets = deserialize_objects(directory.dir("datasets"), RawDataset)
        datafiles = deserialize_objects(directory.dir("datafiles"), RawDatafile)

        return IngestionManifest(
            projects=projects,
            experiments=experiments,
            datasets=datasets,
            datafiles=datafiles,
        )

    def summarize(self, skip_datafiles: bool = True) -> str:
        stream = io.StringIO()

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

        if skip_datafiles:
            stream.write(f"Number of datafiles: {len(self.get_datafiles())}")
        else:
            write_header("Datafiles")
            write_dataclasses(self.get_datafiles())

        return stream.getvalue()
