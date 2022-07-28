"""Helper functions related to multiple dataclasses."""

from typing import List

from src.blueprints import BaseDatafile, BaseDataset, BaseExperiment, BaseProject
from src.blueprints.datafile import RawDatafile, RefinedDatafile
from src.blueprints.dataset import DatasetParameterSet, RawDataset, RefinedDataset
from src.blueprints.experiment import (
    ExperimentParameterSet,
    RawExperiment,
    RefinedExperiment,
)
from src.blueprints.project import ProjectParameterSet
from src.helpers import ObjectEnum
from src.helpers.enumerators import ObjectPostEnum


def get_object_name(
    object_class: BaseDatafile | BaseDataset | BaseExperiment | BaseProject,
) -> str | None:
    """Generic helper function to get the name from a MyTardis  dataclass object"""
    if isinstance(object_class, BaseDatafile):
        return object_class.filename
    if isinstance(object_class, BaseDataset):
        return object_class.description
    if isinstance(object_class, BaseExperiment):
        return object_class.title
    if isinstance(object_class, BaseProject):
        return object_class.name
    return None


def get_object_type(
    object_class: BaseDatafile | BaseDataset | BaseExperiment | BaseProject,
) -> ObjectEnum | None:
    """Generic helper function to get the name from a MyTardis  dataclass object"""
    if isinstance(object_class, BaseDatafile):
        return ObjectEnum.DATAFILE
    if isinstance(object_class, BaseDataset):
        return ObjectEnum.DATASET
    if isinstance(object_class, BaseExperiment):
        return ObjectEnum.EXPERIMENT
    if isinstance(object_class, BaseProject):
        return ObjectEnum.PROJECT
    return None


def get_object_parents(
    object_class: RawDatafile
    | RefinedDatafile
    | RawDataset
    | RefinedDataset
    | RawExperiment
    | RefinedExperiment,
) -> List[str] | None:
    """Function to get the parent objects from teh RawObject classes."""
    if isinstance(object_class, BaseDatafile):
        return [object_class.dataset]
    if isinstance(object_class, BaseDataset):
        return object_class.experiments
    if isinstance(object_class, BaseExperiment):
        return object_class.projects
    return None


def get_object_post_type(  # pylint: disable=too-many-return-statements
    object_class: BaseDatafile
    | BaseDataset
    | BaseExperiment
    | BaseProject
    | ProjectParameterSet
    | ExperimentParameterSet
    | DatasetParameterSet,
) -> ObjectPostEnum | None:
    """Generic helper function to return the parameeters needed to correctly
    POST or PUT/PATCH a MyTardis object."""
    if isinstance(object_class, BaseDatafile):
        return ObjectPostEnum.DATAFILE
    if isinstance(object_class, BaseDataset):
        return ObjectPostEnum.DATASET
    if isinstance(object_class, BaseExperiment):
        return ObjectPostEnum.EXPERIMENT
    if isinstance(object_class, BaseProject):
        return ObjectPostEnum.PROJECT
    if isinstance(object_class, ProjectParameterSet):
        return ObjectPostEnum.PROJECT_PARAMETERS
    if isinstance(object_class, ExperimentParameterSet):
        return ObjectPostEnum.EXPERIMENT_PARAMETERS
    if isinstance(object_class, DatasetParameterSet):
        return ObjectPostEnum.DATASET_PARAMETERS
    return None
