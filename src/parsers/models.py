""" This is the data model from Ingestion Data Wizard which is used to define the YAML structure, and data types.
And how to parse the YAML file into Python objects.
"""
import logging
from dataclasses import dataclass, field
from pathlib import Path  # ## added
from typing import Any, Dict, List, Literal, Optional, Type, TypeAlias, TypeVar, Union

import yaml
from yaml.loader import Loader
from yaml.nodes import MappingNode, Node


class YAMLSerializable(yaml.YAMLObject):
    """
    A base class that provides YAML serialization and deserialization capabilities
    to its subclasses. It uses PyYAML library to convert a YAML representation
    to a Python object.

    Attributes:
    ----------
    None

    Methods:
    -------
    from_yaml(cls: Type, loader: Loader, node: MappingNode) -> Any:
        Convert a representation node to a Python object,
        calling __init__ to create a new object.

        We're using dataclasses to create an __init__ method
        which sets default values for fields not present in YAML document.
        By default, YAMLObject does not call __init__, so yaml.safe_load throws an exception
        on documents that don't have all required fields. (see https://github.com/yaml/pyyaml/issues/510,
        https://stackoverflow.com/questions/13331222/yaml-does-not-call-the-constructor)
        So we override the from_yaml method here to call __init__ (see
        https://stackoverflow.com/questions/7224033/default-constructor-parameters-in-pyyaml)

        Parameters:
        ----------
        cls : Type
            The class that this method belongs to.
        loader : Loader
            A PyYAML loader object that loads the YAML representation of the object.
        node : MappingNode
            A PyYAML node object that represents the YAML mapping that contains the object's data.

        Returns:
        -------
        Any
            A new object created by calling the __init__ method with the values extracted from the YAML node.

    """
    @classmethod
    def from_yaml(cls: Type, loader: Loader, node: MappingNode) -> Any:
        """
        Convert a representation node to a Python object,
        calling __init__ to create a new object.

        We're using dataclasses to create an __init__ method
        which sets default values for fields not present in YAML document.
        By default, YAMLObject does not call __init__, so yaml.safe_load throws an exception
        on documents that don't have all required fields. (see https://github.com/yaml/pyyaml/issues/510,
        https://stackoverflow.com/questions/13331222/yaml-does-not-call-the-constructor)
        So we override the from_yaml method here to call __init__ (see
        https://stackoverflow.com/questions/7224033/default-constructor-parameters-in-pyyaml)

        Parameters:
        ----------
        cls : Type
            The class that this method belongs to.
        loader : Loader
            A PyYAML loader object that loads the YAML representation of the object.
        node : MappingNode
            A PyYAML node object that represents the YAML mapping that contains the object's data.

        Returns:
        -------
        Any
            A new object created by calling the __init__ method with the values extracted from the YAML node.

        """
        fields = loader.construct_mapping(node)
        return cls(**fields)


@dataclass
class IProjectAccessControl:
    """
    A class representing fields related to access
    control. This class represents fields for Projects,
    while the IDerviedAccessControl class represents fields
    for experiments, datasets and datafiles.
    """

    admin_groups: List[str] = field(default_factory=list)
    admin_users: List[str] = field(default_factory=list)
    read_groups: List[str] = field(default_factory=list)
    read_users: List[str] = field(default_factory=list)
    download_groups: List[str] = field(default_factory=list)
    download_users: List[str] = field(default_factory=list)
    sensitive_groups: List[str] = field(default_factory=list)
    sensitive_users: List[str] = field(default_factory=list)


@dataclass
class IDerivedAccessControl:
    """
    A class representing fields related to access
    control. This class represents fields for Experiments,
    Datasets and Datafiles,while the IProjectAccessControl
    class represents fields for Projects.
    When set to None, the fields represent that they are inheriting
    access control fields from the containing object.
    """

    admin_groups: Optional[List[str]] = field(default=None)
    admin_users: Optional[List[str]] = field(default=None)
    read_groups: Optional[List[str]] = field(default=None)
    read_users: Optional[List[str]] = field(default=None)
    download_groups: Optional[List[str]] = field(default=None)
    download_users: Optional[List[str]] = field(default=None)
    sensitive_groups: Optional[List[str]] = field(default=None)
    sensitive_users: Optional[List[str]] = field(default=None)


"""
A union type alias for both types of Access Control types.
"""
IAccessControl: TypeAlias = Union[IProjectAccessControl, IDerivedAccessControl]


@dataclass
class IMetadata:
    """
    A class representing fields related to schema parameters.

    Attributes:
        metadata (Dict[str, Any]): A dictionary representing metadata for the object.
    """
    # change to Optional[]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RawProject(YAMLSerializable, IProjectAccessControl, IMetadata):
    """
    A class representing MyTardis Project objects.

    Attributes:
        description (str): A brief description of the project.
        project_id (str): The unique identifier of the project.
        alternate_ids (List[str]): A list of alternate identifiers for the project.
        lead_researcher (str): The name of the lead researcher for the project.
        name (str): The name of the project.
        principal_investigator (str): The name of the principal investigator for the project.
    """
    yaml_tag = "!Project"
    yaml_loader = yaml.SafeLoader
    description: str = ""
    project_id: str = ""
    alternate_ids: List[str] = field(default_factory=list)
    lead_researcher: str = ""
    name: str = ""
    principal_investigator: str = "abcd123"


@dataclass
class RawExperiment(YAMLSerializable, IDerivedAccessControl, IMetadata):
    """
    A class representing MyTardis Experiment objects.

    Attributes:
        project_id (str): The unique identifier of the project this experiment belongs to.
        experiment_id (str): The unique identifier of the experiment.
        alternate_ids (List[str]): A list of alternate identifiers for the experiment.
        description (str): A brief description of the experiment.
        title (str): The title of the experiment.
    """
    yaml_tag = "!Experiment"
    yaml_loader = yaml.SafeLoader
    project_id: str = ""
    experiment_id: str = ""
    alternate_ids: List[str] = field(default_factory=list)
    description: str = ""
    title: str = ""


@dataclass
class RawDataset(YAMLSerializable, IDerivedAccessControl, IMetadata):
    """
    A class representing MyTardis Dataset objects.

    Attributes:
        dataset_name (str): The name of the dataset.
        experiment_id (List[str]): A list of unique identifiers of the experiments this dataset belongs to.
        dataset_id (str): The unique identifier of the dataset.
        instrument_id (str): The unique identifier of the instrument used to generate the data.
        description (str): A brief description of the dataset.
        instrument (str): The name of the instrument used to generate the data.
        experiments (List[str]): A list of experiment names associated with this dataset.
    """
    yaml_tag = "!Dataset"
    yaml_loader = yaml.SafeLoader
    dataset_name: str = ""
    experiment_id: List[str] = field(default_factory=list)
    dataset_id: str = ""
    instrument_id: str = ""
    description: str = ""  
    instrument: str = ""  
    experiments: List[str] = field(default_factory=list) 


@dataclass
class FileInfo(YAMLSerializable, IDerivedAccessControl, IMetadata):
    """A class representing MyTardis Datafile objects.

    Attributes:
        name (str): The name of the datafile.
        size (int): The size of the datafile.
        filename (str): The filename of the datafile.
        directory (str): The directory path of the datafile.
        md5sum (str): The md5sum hash of the datafile.
        mimetype (str): The mimetype of the datafile.
        dataset (str): The name of the dataset that the datafile belongs to.
        yaml_tag (str): The YAML tag for serialization of this class.
        yaml_loader (yaml.Loader): The YAML loader class for deserialization of this class.
    """
    yaml_tag = "!FileInfo"
    yaml_loader = yaml.SafeLoader
    name: str = ""
    size: int = field(repr=False, default=0)
    filename: str = ""
    directory: str = ""
    md5sum: str = ""
    mimetype: str = ""
    dataset: str = ""


### create new Datafile class to match fields in ingestion script
@dataclass
class RawDatafile(YAMLSerializable, IDerivedAccessControl, IMetadata):
    """
    A class representing MyTardis Datafile objects.

    Attributes:
        size (str): The size of the datafile.
        filename (str): The name of the file.
        directory (str): The directory where the file is located.
        md5sum (str): The MD5 checksum of the file.
        mimetype (str): The MIME type of the file.
        dataset (str): The dataset to which the datafile belongs.
        dataset_id (str): The ID of the dataset to which the datafile belongs.
    """
    yaml_tag = "!Datafile"
    yaml_loader = yaml.SafeLoader
    size: str = ""  
    filename: str = ""
    directory: str = ""
    md5sum: str = ""
    mimetype: str = ""
    dataset: str = ""
    dataset_id: str = ""


@dataclass
class IngestionMetadata:
    """
    A class representing a collection of metadata, with
    objects of different MyTardis types. It can be serialised
    to become a YAML file for ingestion into MyTardis.

    Attributes
    ----------
    projects : List[RawProject]
        A list of RawProject objects.
    experiments : List[RawExperiment]
        A list of RawExperiment objects.
    datasets : List[RawDataset]
        A list of RawDataset objects.
    datafiles : List[RawDatafile]
        A list of RawDatafile objects.
    """
    projects: List[RawProject] = field(default_factory=list)
    experiments: List[RawExperiment] = field(default_factory=list)
    datasets: List[RawDataset] = field(default_factory=list)
    datafiles: List[RawDatafile] = field(default_factory=list)

    def is_empty(self) -> bool:
        """
        Returns whether the IngestionMetadata object is empty.

        Returns
        -------
        bool
            True if the object is empty, False otherwise.
        """
        return (
            len(self.projects) == 0
            and len(self.experiments) == 0
            and len(self.datasets) == 0
            and len(self.datafiles) == 0
        )

    def to_yaml(self):
        """
        Returns a string of the YAML representation of the metadata.

        Returns
        -------
        str
            A string of the YAML representation of the metadata.
        """
        concatenated: List[Any] = self.projects
        concatenated.extend(self.experiments)
        concatenated.extend(self.datasets)
        concatenated.extend(self.datafiles)
        yaml_file = yaml.dump_all(concatenated)
        return yaml_file

    def get_files_by_dataset(self, dataset: RawDataset) -> List[RawDatafile]:
        """
        Returns datafiles that belong to a dataset.

        Parameters
        ----------
        dataset : RawDataset
            The dataset to which the datafiles should belong.

        Returns
        -------
        List[RawDatafile]
            A list of RawDatafile objects that belong to the given dataset.
        """
        id = dataset.dataset_id
        # update with Datafile
        all_files: List[RawDatafile] = []
        for file in self.datafiles:
            if not file.dataset_id == id:
                continue
            # Concatenate list of fileinfo matching dataset
            # with current list
            all_files.append(file)
        return all_files

    def get_datasets_by_experiment(self, exp: RawExperiment) -> List[RawDataset]:
        """
        Returns datasets that belong to an experiment.

        Parameters
        ----------
        exp : RawExperiment
            An instance of RawExperiment to get the datasets for.

        Returns
        -------
        List[RawDataset]
            A list of RawDataset instances that belong to the experiment.
        """
        id = exp.experiment_id
        all_datasets: List[RawDataset] = []
        for dataset in self.datasets:
            if id not in dataset.experiment_id:
                continue
            all_datasets.append(dataset)
        return all_datasets

    def get_experiments_by_project(self, proj: RawProject) -> List[RawExperiment]:
        """
        Returns experiments that belong to a project.

        Parameters
        ----------
        proj : RawProject
            An instance of RawProject to get the experiments for.

        Returns
        -------
        List[RawExperiment]
            A list of RawExperiment instances that belong to the project.
        """
        id = proj.project_id
        all_exps: List[RawExperiment] = []
        for exp in self.experiments:
            if not exp.project_id == id:
                continue
            all_exps.append(exp)
        return all_exps

    @staticmethod
    def from_yaml(yaml_rep: str):
        """Returns a IngestionMetadata object by loading metadata from content of a YAML file.

        Parameters
        ----------
        yaml_rep : str
            The content of a YAML file. Note that this is the content, not the path of the file.
            The function does not read from a file for you, you have to pass in the file's content.

        Returns
        -------
        IngestionMetadata
            An instance of IngestionMetadata populated with objects from the YAML representation.
        """
        metadata = IngestionMetadata()
        objects = yaml.safe_load_all(yaml_rep)
        # Iterate through all the objects,
        # sorting them into the right list
        # based on type.
        for obj in objects:
            if isinstance(obj, RawProject):
                metadata.projects.append(obj)
            elif isinstance(obj, RawExperiment):
                metadata.experiments.append(obj)
            elif isinstance(obj, RawDataset):
                metadata.datasets.append(obj)
            elif isinstance(obj, RawDatafile):
                metadata.datafiles.append(obj)
            else:
                logging.warning(
                    "Encountered unknown object while reading YAML"
                    + ", ignored. Object was %s",
                    obj,
                )
        # print(metadata.datafiles)
        return metadata


# # To use:
# dataset = Dataset()
# dataset.dataset_name = "Calibration 10 X"
# dataset.dataset_id = "2022-06-calibration-10-x"

# # To dump into YAML
# yaml.dump(dataset)

# # To dump multiple objects
# datafile = Datafile()
# datafile.dataset_id = "2022-06-calibration-10-x"
# output = [dataset, datafile]
# yaml.dump_all(output)

# # If not working with tags, strip them out and process as normal.
# import yaml
# from yaml.nodes import MappingNode, ScalarNode, SequenceNode
# def strip_unknown_tag_and_construct(loader, node):
#     node.tag = ""
#     # print(node)
#     if isinstance(node, ScalarNode):
#         return loader.construct_scalar(node)
#     if isinstance(node, SequenceNode):
#         return loader.construct_sequence(node)
#     if isinstance(node, MappingNode):
#         return loader.construct_mapping(node)
#     else:
#         return None

# yaml.SafeLoader.add_constructor(None, strip_unknown_tag_and_construct)
# with open('test/test.yaml') as f:
#     a = list(yaml.safe_load_all(f))