""" This is the data model from Ingestion Data Wizard which is used to define the YAML structure, and data types.
And how to parse the YAML file into Python objects.
"""
from typing import List, Dict, Any, Literal, Optional, Type, TypeAlias, TypeVar, Union
from dataclasses import dataclass, field
import yaml
from yaml.loader import Loader
from yaml.nodes import Node
import logging
from pathlib import Path ### added

class YAMLSerializable(yaml.YAMLObject):
    @classmethod
    def from_yaml(cls: Type, loader: Loader, node: Node) -> Any:
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
IAccessControl:TypeAlias = Union[IProjectAccessControl, IDerivedAccessControl]    

@dataclass
class IMetadata:
    """
    A class representing fields related to schema parameters.
    """
    # change to Optional[]
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class RawProject(YAMLSerializable, IProjectAccessControl, IMetadata):
    """
    A class representing MyTardis Project objects.
    """

    yaml_tag = "!Project"
    yaml_loader = yaml.SafeLoader
    # yaml_dumper = yaml.SafeDumper
    #project_name: str = ""
    description: str = ""
    project_id: str = ""
    alternate_ids: List[str] = field(default_factory=list)
    description: str = ""
    lead_researcher: str = ""
    ## fields below were added
    name: str = ""
    principal_investigator: str = "abcd123"


@dataclass
class RawExperiment(YAMLSerializable, IDerivedAccessControl, IMetadata):
    """
    A class representing MyTardis Experiment objects.
    """

    yaml_tag = "!Experiment"
    yaml_loader = yaml.SafeLoader
    # yaml_dumper = yaml.SafeDumper
    # experiment_name: str = ""
    project_id: str = ""
    experiment_id: str = ""
    alternate_ids: List[str] = field(default_factory=list)
    description: str = ""
    ### fields below were added
    title: str = ""


@dataclass
class RawDataset(YAMLSerializable, IDerivedAccessControl, IMetadata):
    """
    A class representing MyTardis Dataset objects.
    """

    yaml_tag = "!Dataset"
    yaml_loader = yaml.SafeLoader
    # yaml_dumper = yaml.SafeDumper
    dataset_name: str = ""
    experiment_id: List[str] = field(default_factory=list)
    dataset_id: str = ""
    instrument_id: str = ""
    ## fields below were added
    description: str = "" ## description field was added
    instrument: str = "" ## instrument field was added
    experiments: List[str] = field(default_factory=list) ## experiments field was added


@dataclass
class FileInfo(YAMLSerializable, IDerivedAccessControl, IMetadata):
    """
    A class representing MyTardis Datafile objects.
    """
    yaml_tag = "!FileInfo"
    yaml_loader = yaml.SafeLoader
    name: str = "" 
    # Size property is not serialised.
    size: int = field(repr=False, default=0)
    ### fields below were added
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
    """
    yaml_tag = "!Datafile"
    yaml_loader = yaml.SafeLoader
    # yaml_dumper = yaml.SafeDumper
    #name: str = "" 
    # Size property is not serialised.
    size: float = ""
    ### fields below were added
    filename: str = ""
    directory: str = ""
    md5sum: str = ""
    mimetype: str = ""
    dataset: str = ""
    dataset_id: str = ""
'''
@dataclass
class Datafile(YAMLSerializable):
    """
    A class representing a set of MyTardis datafile objects.
    """

    yaml_tag = "!Datafile"
    yaml_loader = yaml.SafeLoader
    # yaml_dumper = yaml.SafeDumper
    dataset_id: str = ""
    files: List[FileInfo] = field(default_factory=list) 
    ## fields below were added
    filename: str = ""
    directory: str = ""
    md5sum: str = ""
    mimetype: str = ""
    dataset: str = ""
    size: int = field(repr=False, default=0)
'''


@dataclass
class IngestionMetadata:
    """
    A class representing a collection of metadata, with
    objects of different MyTardis types. It can be serialised
    to become a YAML file for ingestion into MyTardis.
    """

    # A list of objects of each type.
    projects: List[RawProject] = field(default_factory=list)
    experiments: List[RawExperiment] = field(default_factory=list)
    datasets: List[RawDataset] = field(default_factory=list)
    datafiles: List[RawDatafile] = field(default_factory=list)        

    def is_empty(self) -> bool:
        return (len(self.projects) == 0 and
            len(self.experiments) == 0 and
            len(self.datasets) == 0 and
            len(self.datafiles) == 0
        )

    def to_yaml(self):
        """
        Returns a string of the YAML representation of the metadata.
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
        """
        id = dataset.dataset_id
        #update with Datafile
        all_files: List[RawDatafile] = []
        # all_files: List[FileInfo] = []
        for file in self.datafiles:
            if not file.dataset_id == id:
                continue
            # Concatenate list of fileinfo matching dataset
            # with current list
            all_files.append(file)
        return all_files

    def get_datasets_by_experiment(self, exp: RawExperiment) -> List[RawDataset]:
        """
        Returns datasets that belong to a experiment.
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
        #print(metadata.datafiles)
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
