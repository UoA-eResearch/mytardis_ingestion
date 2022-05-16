"""NB: THIS WILL NOT WORK AS IT IS AND IS PROVIDED FOR INDICATIVE PURPOSES ONLY

Script to create the objects in MyTardis.

The workflow for these scripts are as follows:
- Read in the YAML files and determine which contain Projects, Experiments, Datasets and Datafiles
- Process the files in descending order of Hierarchy in order to maximise the chance that parent
    objects exist
- In the processing phase, check to see if an object with the identifier exists (TODO write code for
    when no identifier app is being used)
- If the object exists check to see that the metadata is the same and report if not or overwrite
    using a PUT request if specifically requested.
- Process the YAML into object and parameter dictionaries which will require additional calls to
    ensure that related objects exist
- Using the Forge create the object within MyTardis
- Move files from their current location to their final location
- Collate successes and failures and prepare a report to be emailed
"""

import logging
import subprocess
from pathlib import Path

from src.specific_implementations import YAMLIngestionFactory

logger = logging.getLogger(__name__)

config_dict = {
    "username": "MyTardis username",
    "api_key": "TastyPie API key",
    "hostname": "MyTardis URL",
    "verify_certificate": True,
    "default_institution": "Institution ID/Name",
    "default_schema": {
        "project": "https://test.com/project/v1",
        "experiment": "https://test.com/experiment/v1",
        "dataset": "https://test.com/dataset/v1",
        "datafile": "https://test.com/datafile/v1",
    },
    "source_directory": "Directory path to where the data files are mounted",
    "target_directory": "Directory stub to the file target in MyTardis",
    "storage_box": "MyTardis storage pathe",
}
yaml_path = Path("path to YAML files")

factory = YAMLIngestionFactory(config_dict)

project_files = factory.build_object_lists(yaml_path, "project")
for project_file in project_files:
    factory.process_projects(project_file)

experiment_files = factory.build_object_lists(yaml_path, "experiment")
for experiment_file in experiment_files:
    factory.process_experiments(experiment_file)

dataset_files = factory.build_object_lists(yaml_path, "dataset")
for dataset_file in dataset_files:
    factory.process_datasets(dataset_file)

datafile_files = factory.build_object_lists(yaml_path, "datafile")
for datafile_file in datafile_files:
    for dictionary in factory.smelter.read_file(datafile_file):
        for filename in dictionary["datafiles"]["files"]:
            raw_path = Path(filename["name"]).relative_to(
                Path("Root directory of the source host")
            )
            file_path = (  # pylint: disable=invalid-name
                Path("target directory").as_posix() + "/./" + raw_path.as_posix()
            )
            target_path = Path("root directory of MyTardis storage") / raw_path
            command_string = (
                f"rsync -avz --relative {file_path} user@test.com:path to storage"
            )
            subprocess.run(command_string, shell=True, check=True)  # nosec
            # Note the subprocess call is excluded from the bandit check via nosec as this is
            # intended to demonstrate how the file movement needs to occur. In practice this
            # should probably be handled by a shell script that also calls the ingestion process
    factory.process_datafiles(datafile_file)
