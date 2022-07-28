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
from src.helpers.enumerators import ObjectDict, ObjectPostDict, ObjectPostEnum


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
) -> ObjectDict | None:
    """Generic helper function to get the name from a MyTardis  dataclass object"""
    if isinstance(object_class, BaseDatafile):
        return ObjectEnum.DATAFILE.value
    if isinstance(object_class, BaseDataset):
        return ObjectEnum.DATASET.value
    if isinstance(object_class, BaseExperiment):
        return ObjectEnum.EXPERIMENT.value
    if isinstance(object_class, BaseProject):
        return ObjectEnum.PROJECT.value
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
) -> ObjectPostDict | None:
    """Generic helper function to return the parameeters needed to correctly
    POST or PUT/PATCH a MyTardis object."""

    if isinstance(object_class, BaseDatafile):
        return ObjectPostEnum.DATAFILE.value
    if isinstance(object_class, BaseDataset):
        return ObjectPostEnum.DATASET.value
    if isinstance(object_class, BaseExperiment):
        return ObjectPostEnum.EXPERIMENT.value
    if isinstance(object_class, BaseProject):
        return ObjectPostEnum.PROJECT.value
    if isinstance(object_class, ProjectParameterSet):
        return ObjectPostEnum.PROJECT_PARAMETERS.value
    if isinstance(object_class, ExperimentParameterSet):
        return ObjectPostEnum.EXPERIMENT_PARAMETERS.value
    if isinstance(object_class, DatasetParameterSet):
        return ObjectPostEnum.DATASET_PARAMETERS.value
    return None
