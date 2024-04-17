"""Helper functions related to multiple dataclasses."""

from src.blueprints.datafile import BaseDatafile
from src.blueprints.dataset import BaseDataset, DatasetParameterSet
from src.blueprints.experiment import BaseExperiment, ExperimentParameterSet
from src.blueprints.project import BaseProject, ProjectParameterSet
from src.mytardis_client.enumerators import ObjectPostDict, ObjectPostEnum


def get_object_post_type(  # pylint: disable=too-many-return-statements
    object_class: (
        BaseDatafile
        | BaseDataset
        | BaseExperiment
        | BaseProject
        | ProjectParameterSet
        | ExperimentParameterSet
        | DatasetParameterSet
    ),
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
