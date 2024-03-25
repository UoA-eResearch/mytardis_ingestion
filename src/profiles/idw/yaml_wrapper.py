"""
YAML Writer Module

This module is used for writing IngestibleDataclasses to a YAML file.

The write_to_yaml function takes a file path and an IngestibleDataclasses object
and writes the data to the specified YAML file.

Usage:
    To write IngestibleDataclasses to a YAML file:
        write_to_yaml(fpath, ingestible)
"""

# Standard library imports
from pathlib import Path
from typing import List, Union

# Third-party imports
import yaml

# User-defined imports
from src.blueprints import RawDatafile, RawDataset, RawExperiment, RawProject
from src.extraction.manifest import IngestionManifest


def write_to_yaml(fpath: Path, ingestible: IngestionManifest) -> None:
    """
    Write the IngestibleDataclasses to a YAML file.

    Args:
        fpath (Path): The path to the YAML file.
        ingestible (IngestibleDataclasses): The data to write to the YAML file.
    """
    data_list: List[Union[RawProject, RawExperiment, RawDataset, RawDatafile]] = []
    for project in ingestible.get_projects():
        data_list.append(project)
    for experiment in ingestible.get_experiments():
        data_list.append(experiment)
    for dataset in ingestible.get_datasets():
        data_list.append(dataset)
    for datafile in ingestible.get_datafiles():
        data_list.append(datafile)

    with open(fpath, "w", encoding="utf-8") as f:
        yaml.dump_all(data_list, f)
