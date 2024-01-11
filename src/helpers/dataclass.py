# pylint: disable=R0801
"""Helper functions related to multiple dataclasses."""

from typing import List

from src.blueprints.datafile import BaseDatafile, RawDatafile, RefinedDatafile
from src.blueprints.dataset import (
    BaseDataset,
    DatasetParameterSet,
    RawDataset,
    RefinedDataset,
)
from src.blueprints.experiment import (
    BaseExperiment,
    ExperimentParameterSet,
    RawExperiment,
    RefinedExperiment,
)
from src.blueprints.project import (
    BaseProject,
    ProjectParameterSet,
    RawProject,
    RefinedProject,
)
from src.mytardis_client.enumerators import (
    ObjectDict,
    ObjectEnum,
    ObjectPostDict,
    ObjectPostEnum,
)


def get_object_name(
    object_class: BaseDatafile | BaseDataset | BaseExperiment | BaseProject,
) -> str:  # sourcery skip: assign-if-exp, reintroduce-else
    """Generic helper function to get the name from a MyTardis  dataclass object"""
    if isinstance(object_class, BaseDatafile):
        return object_class.filename
    if isinstance(object_class, BaseDataset):
        return object_class.description
    if isinstance(object_class, BaseExperiment):
        return object_class.title
    if isinstance(object_class, BaseProject):
        return object_class.name
    return ""


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
    | RefinedExperiment
    | RawProject
    | RefinedProject,
) -> List[str] | None:  # sourcery skip: assign-if-exp, reintroduce-else
    """Function to get the parent objects from teh RawObject classes."""
    if isinstance(object_class, BaseDatafile):
        return [object_class.dataset]
    if isinstance(object_class, BaseDataset):
        return object_class.experiments
    if isinstance(object_class, BaseExperiment):
        return object_class.projects
    if isinstance(object_class, BaseProject):
        return None
    return []


def get_object_post_type(  # pylint: disable=too-many-return-statements
    object_class: BaseDatafile
    | BaseDataset
    | BaseExperiment
    | BaseProject
    | ProjectParameterSet
    | ExperimentParameterSet
    | DatasetParameterSet,
) -> ObjectPostDict:
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

    raise TypeError(f"POST type undefined for object {object_class}")
