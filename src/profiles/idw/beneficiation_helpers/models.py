# pylint: disable-all
# type: ignore
# This needs a lot of refactoring so disable checks

""" This is the data model from Ingestion Data Wizard which is used to define the YAML structure, and data types.
And how to parse the YAML file into Python objects.
"""
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Type, TypeAlias, TypeVar, Union

import yaml
from yaml.loader import Loader
from yaml.nodes import MappingNode, Node

from src.blueprints.custom_data_types import URI, ISODateTime, Username
from src.helpers.enumerators import DataClassification


class YAMLSerializable(yaml.YAMLObject):
    """
    A base class that provides YAML serialization and deserialization capabilities
    to its subclasses. It uses PyYAML library to convert a YAML representation
    to a Python object.

    Attributes:
        None

    Methods:
        from_yaml(cls: Type, loader: Loader, node: MappingNode) -> Any: A class method that loads and constructs an object from a YAML node.
    """

    @classmethod
    def from_yaml(cls: Type, loader: Loader, node: MappingNode) -> Any:
        """
        Loads and constructs an object from a YAML node.

        Args:
            cls (Type): The class that needs to be constructed.
            loader (Loader): The YAML loader instance.
            node (MappingNode): The YAML mapping node containing the data.

        Returns:
            Any: An instance of the class.
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
        name (str): The name of the project.
        description (str): A brief description of the project.
        identifiers (List[str]): A list of identifiers for the project.
        data_classification (DataClassification): The data classification of the project.
        principal_investigator (str): The name of the principal investigator for the project.
    """

    yaml_tag = "!Project"
    yaml_loader = yaml.SafeLoader
    name: str = ""
    description: str = ""
    principal_investigator: str = "abcd123"
    data_classification: DataClassification = DataClassification.SENSITIVE
    identifiers: List[str] = field(default_factory=list)


@dataclass
class RawExperiment(YAMLSerializable, IDerivedAccessControl, IMetadata):
    """
    A class representing MyTardis Experiment objects.

    Attributes:
        title (str): The title of the experiment.
        description (str): A brief description of the experiment.
        data_classification (DataClassification): The data classification of the experiment.
        identifiers (List[str]): A list of identifiers for the experiment.
        projects (List[str]): A list of project names associated with this experiment.
    """

    yaml_tag = "!Experiment"
    yaml_loader = yaml.SafeLoader
    title: str = ""
    description: str = ""
    data_classification: Optional[DataClassification] = None
    identifiers: List[str] = field(default_factory=list)
    projects: List[str] = field(default_factory=list)


@dataclass
class RawDataset(YAMLSerializable, IDerivedAccessControl, IMetadata):
    """
    A class representing MyTardis Dataset objects.

    Attributes:
        description (str): The name of the dataset.
        data_classification (DataClassification): The data classification of the dataset.
        identifiers (List[str]): A list of identifiers for the dataset.
        experiments (List[str]): A list of experiment names associated with this dataset.
        instrument (str): The name of the instrument used to generate the data.
    """

    yaml_tag = "!Dataset"
    yaml_loader = yaml.SafeLoader
    description: str = ""
    data_classification: Optional[DataClassification] = None
    identifiers: List[str] = field(default_factory=list)
    experiments: List[str] = field(default_factory=list)
    instrument: str = ""


### create new Datafile class to match fields in ingestion script
@dataclass
class RawDatafile(YAMLSerializable, IDerivedAccessControl, IMetadata):
    """
    A class representing MyTardis Datafile objects.

    Attributes:
        filename (str): The name of the file.
        directory (str): The directory where the file is located.
        md5sum (str): The MD5 checksum of the file.
        mimetype (str): The MIME type of the file.
        size (str): The size of the datafile.
        dataset (str): The dataset to which the datafile belongs.
    """

    yaml_tag = "!Datafile"
    yaml_loader = yaml.SafeLoader
    filename: str = ""
    directory: Path = ""
    md5sum: str = ""
    mimetype: str = ""
    size: int = field(repr=False, default=0)
    dataset: str = ""


@dataclass
class IngestionMetadata:
    """
    A class to represent the metadata associated with ingesting data.

    Attributes:
        projects (List[RawProject]): A list of RawProject objects.
        experiments (List[RawExperiment]): A list of RawExperiment objects.
        datasets (List[RawDataset]): A list of RawDataset objects.
        datafiles (List[RawDatafile]): A list of RawDatafile objects.
    """

    projects: List[RawProject] = field(default_factory=list)
    experiments: List[RawExperiment] = field(default_factory=list)
    datasets: List[RawDataset] = field(default_factory=list)
    datafiles: List[RawDatafile] = field(default_factory=list)

    def is_empty(self) -> bool:
        """
        Check if the metadata is empty.

        Returns:
            bool: True if there are no projects, experiments, datasets or datafiles, False otherwise.
        """
        return (
            len(self.projects) == 0
            and len(self.experiments) == 0
            and len(self.datasets) == 0
            and len(self.datafiles) == 0
        )

    def to_yaml(self) -> None:
        """
        Convert the metadata to a YAML string.

        Returns:
            str: A YAML representation of the metadata.
        """
        concatenated: List[Any] = self.projects
        concatenated.extend(self.experiments)
        concatenated.extend(self.datasets)
        concatenated.extend(self.datafiles)
        yaml_file = yaml.dump_all(concatenated)
        return yaml_file

    def get_files_by_dataset(self, dataset: RawDataset) -> List[RawDatafile]:
        """
        Get the datafiles associated with a given dataset.

        Args:
            dataset (RawDataset): The dataset to look for.

        Returns:
            List[RawDatafile]: A list of RawDatafile objects associated with the dataset.
        """
        id = dataset.identifiers[0]
        all_files: List[RawDatafile] = []
        for file in self.datafiles:
            if not file.dataset == id:
                continue
            all_files.append(file)
        return all_files

    def get_datasets_by_experiment(self, exp: RawExperiment) -> List[RawDataset]:
        """
        Get the datasets associated with a given experiment.

        Args:
            exp (RawExperiment): The experiment to look for.

        Returns:
            List[RawDataset]: A list of RawDataset objects associated with the experiment.
        """
        id = exp.identifiers[0]
        all_datasets: List[RawDataset] = []
        for dataset in self.datasets:
            if id not in dataset.experiments:
                continue
            all_datasets.append(dataset)
        return all_datasets

    def get_experiments_by_project(self, proj: RawProject) -> List[RawExperiment]:
        """
        Get the experiments associated with a given project.

        Args:
            proj (RawProject): The project to look for.

        Returns:
            List[RawExperiment]: A list of RawExperiment objects associated with the project.
        """
        id = proj.identifiers[0]
        all_exps: List[RawExperiment] = []
        for exp in self.experiments:
            if not exp.projects == id:
                continue
            all_exps.append(exp)
        return all_exps

    @staticmethod
    def from_yaml(yaml_rep: str) -> None:
        """
        This static method converts a YAML string to an IngestionMetadata object.

        Args:
            yaml_rep (str): A string in YAML format representing the metadata to be converted.

        Returns:
            IngestionMetadata: An IngestionMetadata object containing the metadata from the input YAML string.
        """
        metadata = IngestionMetadata()
        objects = yaml.safe_load_all(yaml_rep)
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
        return metadata


"""
Usage:
dataset = Dataset()
dataset.dataset_name = "Calibration 10 X"
dataset.dataset_id = "2022-06-calibration-10-x"

python
Copy code
# To dump into YAML
yaml.dump(dataset)

# To dump multiple objects
datafile = Datafile()
datafile.dataset_id = "2022-06-calibration-10-x"
output = [dataset, datafile]
yaml.dump_all(output)

# If not working with tags, strip them out and process as normal.
import yaml
from yaml.nodes import MappingNode, ScalarNode, SequenceNode

def strip_unknown_tag_and_construct(loader, node):
    node.tag = ""
    if isinstance(node, ScalarNode):
        return loader.construct_scalar(node)
    if isinstance(node, SequenceNode):
        return loader.construct_sequence(node)
    if isinstance(node, MappingNode):
        return loader.construct_mapping(node)
    else:
        return None

yaml.SafeLoader.add_constructor(None, strip_unknown_tag_and_construct)
with open('test/test.yaml') as f:
    a = list(yaml.safe_load_all(f))
"""
