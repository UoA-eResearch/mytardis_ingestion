# pylint: disable-all
"""A model-like class that is designed to contain the raw dataclasses for
refinery/ingestion. The raw dataclasses are stored in lists.
"""

# ---Imports
from __future__ import annotations

import io
import json
import logging
from pathlib import Path
from typing import List, Optional, Sequence, Type, TypeVar

from pydantic import BaseModel
from rich.progress import Progress

from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.utils.filesystem.filesystem_nodes import DirectoryNode
from src.utils.notifiers import ProgressUpdater

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


ModelT = TypeVar("ModelT", bound=BaseModel)


# ---Code
class IngestionManifest:
    """
    Class to record extracted metadata to be ingested.

    It provides methods to add and get projects, experiments, datasets and data files.

    Attributes:
        data_root (Path): The root directory of the data files.
        projects (List[RawProject]): List of projects.
        experiments (List[RawExperiment]): List of experiments.
        datasets (List[RawDataset]): List of datasets.
        datafiles (List[RawDatafile]): List of data files.
    """

    def __init__(
        self,
        source_data_root: Path,
        projects: Optional[list[RawProject]] = None,
        experiments: Optional[list[RawExperiment]] = None,
        datasets: Optional[list[RawDataset]] = None,
        datafiles: Optional[list[RawDatafile]] = None,
    ) -> None:
        self._source_data_root = source_data_root
        self._projects = projects or []
        self._experiments = experiments or []
        self._datasets = datasets or []
        self._datafiles = datafiles or []

    def get_data_root(self) -> Path:
        """Return the root directory for the datafiles."""
        return self._source_data_root

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

        source_info = {
            "source_data_root": self.get_data_root().as_posix(),
        }

        with (root_dir / "source.json").open("w", encoding="utf-8") as f:
            json_data = json.dumps(source_info, indent=4)
            f.write(json_data)

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
    def deserialize(
        root_dir: Path, notifier: Optional[ProgressUpdater]
    ) -> IngestionManifest:
        try:
            directory = DirectoryNode(root_dir)
        except NotADirectoryError as e:
            e.strerror = f"Failed to deserialize ingestion manifest: {e.strerror}"
            raise e

        source_info_file = directory.file("source.json")
        with source_info_file.path().open("r", encoding="utf-8") as f:
            source_info = json.load(f)
            source_data_root = Path(source_info["source_data_root"])

        def deserialize_objects(
            root_dir: DirectoryNode, object_type: Type[ModelT], pedd_type: str
        ) -> list[ModelT]:
            objects = []

            # with Progress() as progress:
            # task_id = progress.add_task(f"Discovering {pedd_type}", total=None)

            obj_dir = root_dir.dir(pedd_type)
            json_files = obj_dir.find_files(lambda p: p.extension() == ".json")

            # progress.update(
            # task_id, description=f"Loading {pedd_type}", total=len(json_files)
            # )

            # if notifier:
            #     notifier.init(f"{pedd_type}", len(json_files))

            for i, file in enumerate(json_files):
                with file.path().open("r", encoding="utf-8") as f:
                    json_data = f.read()
                    obj = object_type.model_validate_json(json_data)
                    objects.append(obj)
                    # progress.update(task_id, advance=1)
                    # if notifier:
                    #     notifier.update(f"{pedd_type}", 1)
                    if len(json_files) % 100 == 0:
                        logger.info(f"Loaded {i}/{len(json_files)} {pedd_type}")

            return objects

        projects = deserialize_objects(directory, RawProject, "projects")
        experiments = deserialize_objects(directory, RawExperiment, "experiments")
        datasets = deserialize_objects(directory, RawDataset, "datasets")
        datafiles = deserialize_objects(directory, RawDatafile, "datafiles")

        return IngestionManifest(
            source_data_root=source_data_root,
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

        write_header("Source Data Info")
        stream.write(f"Data Root: {self.get_data_root().as_posix()}\n")

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
