"""Helper functions related to multiple dataclasses."""

from typing import Type

from src.blueprints import BaseDatafile, BaseDataset, BaseExperiment, BaseProject
from src.helpers import MyTardisObjectEnum


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
) -> MyTardisObjectEnum | None:
    """Generic helper function to get the name from a MyTardis  dataclass object"""
    if isinstance(object_class, BaseDatafile):
        return MyTardisObjectEnum.project
    if isinstance(object_class, BaseDataset):
        return MyTardisObjectEnum.experiment
    if isinstance(object_class, BaseExperiment):
        return MyTardisObjectEnum.dataset
    if isinstance(object_class, BaseProject):
        return MyTardisObjectEnum.datafile
    return None
