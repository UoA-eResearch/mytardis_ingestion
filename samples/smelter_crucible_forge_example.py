# pylint: disable=missing-module-docstring, missing-function-docstring, redefined-outer-name

import logging
import sys
from pathlib import Path
from typing import Dict

from pydantic import BaseModel

from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.config.config import ConfigFromEnv
from src.crucible.crucible import Crucible
from src.forges.forge import Forge
from src.helpers.enumerators import DataClassification
from src.helpers.mt_rest import MyTardisRESTFactory
from src.overseers.overseer import Overseer
from src.smelters.smelter import Smelter

root = logging.getLogger()
root.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(filename="upload_example.log", mode="w")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter("[%(levelname)s]: %(message)s"))

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter("[%(levelname)s]: %(message)s"))

root.addHandler(file_handler)
root.addHandler(console_handler)


#####################################################################################


def print_field_comparison(models: Dict[str, BaseModel]):
    jsons = {name: model.model_dump() for name, model in models.items()}

    fields = set()
    for json in jsons.values():
        for key in json.keys():
            fields.add(key)

    ordered_fields = sorted(list(fields))

    model_names = list(models.keys())
    model_names.insert(0, "")

    display_limit = 40

    rows = [model_names]
    rows.append(["-" * display_limit] * len(model_names))

    for field_name in ordered_fields:
        row = [field_name]

        for json in jsons.values():
            value = json.get(field_name)
            row.append(str(value) if value is not None else "-")

        rows.append(row)

    for row in rows:
        print("| ", end="")
        for entry in row:
            display = entry[0:display_limit].ljust(display_limit)
            print(display, " | ", end="")
        print("")


#####################################################################################
# PROJECT


def create_project_example(smelter: Smelter, crucible: Crucible, forge: Forge) -> None:
    raw_project: RawProject = RawProject(
        name="SCF Example Project",
        description="A test project for working through a smelter-crucible-forge example",
        principal_investigator="awil308",
        data_classification=DataClassification.PUBLIC,
        created_by=None,
        url=None,
        users=None,
        groups=None,
        identifiers=["scf_example"],
        institution=["University of Auckland"],
        # institution=['https://ror.org/03b94tp07'],
        metadata=None,
        schema=None,
        start_time=None,
        end_time=None,
        embargo_until=None,
        active_stores=None,
        archives=None,
        delete_in_days=-1,
        archive_in_days=-1,
    )

    smelt_result = smelter.smelt_project(raw_project)
    assert smelt_result is not None, "Something went wrong during smelting"

    refined_project, parameters = smelt_result

    # print('Refined Project:\n', refined_project.model_dump_json(indent=3))
    # print('Parameters\n:', parameters.model_dump_json(indent=3) if parameters else 'None')

    project = crucible.prepare_project(refined_project)
    assert project is not None, "Crucible stage failed"

    uri = forge.forge_project(refined_object=project, project_parameters=parameters)
    assert uri is not None, "Failed to forge project"

    print("Forged project with URI: ", uri)
    print_field_comparison(
        {
            "RawProject": raw_project,
            "RefinedProject": refined_project,
            "Project": project,
        }
    )


#####################################################################################
# EXPERIMENT


def create_experiment_example(smelter: Smelter, crucible: Crucible, forge: Forge):
    raw_experiment = RawExperiment(
        title="SCF Example Experiment",
        description="A dummy experiment for trying out the API",
        data_classification=DataClassification.PUBLIC,
        created_by="awil308",
        url=None,
        locked=False,
        users=None,
        groups=None,
        identifiers=[],
        projects=["SCF Example Project"],
        institution_name="University of Auckland",
        # institution=['https://ror.org/03b94tp07'],
        metadata={},
        schema=None,
        start_time=None,
        end_time=None,
        created_time=None,
        update_time=None,
        embargo_until=None,
    )

    smelt_result = smelter.smelt_experiment(raw_experiment)
    assert smelt_result is not None, "Smelting failed"

    refined_experiment, parameters = smelt_result

    experiment = crucible.prepare_experiment(refined_experiment)
    assert experiment is not None, "Experiment creation failed"

    uri = forge.forge_experiment(
        refined_object=experiment, experiment_parameters=parameters
    )
    assert uri is not None, "Failed to forge experiment"
    print("Forged experiment with URI: ", uri, ", and parameters: ", parameters)

    print_field_comparison(
        {
            "RawExperiment": raw_experiment,
            "RefinedExperiment": refined_experiment,
            "Experiment": experiment,
        }
    )


#####################################################################################
# DATASET


def create_dataset_example(smelter: Smelter, crucible: Crucible, forge: Forge):
    raw_dataset = RawDataset(
        description="SCF Example Dataset",
        data_classification=DataClassification.PUBLIC,
        directory=None,
        users=None,
        groups=None,
        immutable=False,
        identifiers=None,
        experiments=["SCF Example Experiment"],
        instrument="Dummy Microscope",
        metadata={"image_width": 1921, "image_height": 1082, "num_channels": 4},
        schema="http://andrew-test.com/ds-param-schema/1",
        created_time=None,
        modified_time=None,
    )

    smelt_result = smelter.smelt_dataset(raw_dataset)
    assert smelt_result is not None, "Smelting dataset failed"

    refined_dataset, parameters = smelt_result
    assert smelt_result is not None
    assert parameters is not None

    dataset = crucible.prepare_dataset(refined_dataset)
    assert dataset is not None, "Dataset preparation failed"

    uri = forge.forge_dataset(refined_object=dataset, dataset_parameters=parameters)
    assert uri is not None, "Failed to forge dataset"
    print("Forged dataset with URI: ", uri)
    print("")

    print_field_comparison(
        {
            "RawDataset": raw_dataset,
            "RefinedDataset": refined_dataset,
            "Dataset": dataset,
        }
    )

    print("")
    print("Metadata:\n", parameters.model_dump_json(indent=3))


#####################################################################################
# DATAFILE


def create_datafile_example(smelter: Smelter, crucible: Crucible, forge: Forge):
    metadata = {
        "image_width": 1920,
        "image_height": 1080,
        "num_channels": 3,
    }

    raw_datafile = RawDatafile(
        filename="dummy_image.png",
        directory=Path("a/dummy/dir/dummy_image.png"),
        md5sum="a345bcf3489e8dd8e8a823b01cc834f2",
        mimetype="image/png",
        size=3474853,
        users=None,
        groups=None,
        dataset="SCF Example Dataset",
        metadata=metadata,
        schema="http://andrew-test.com/df-param-schema/1",
    )

    refined_datafile = smelter.smelt_datafile(raw_datafile)
    assert refined_datafile is not None, "Invalid output from datafile smelter"

    datafile = crucible.prepare_datafile(refined_datafile)
    assert datafile is not None, "Invalid output from datafile crucible"

    forge.forge_datafile(datafile)

    print("Forged datafile")
    print_field_comparison(
        {
            "RawDatafile": raw_datafile,
            "RefinedDatafile": refined_datafile,
            "Datafile": datafile,
        }
    )


if __name__ == "__main__":
    config: ConfigFromEnv = ConfigFromEnv()

    rest_client = MyTardisRESTFactory(auth=config.auth, connection=config.connection)

    overseer: Overseer = Overseer(rest_factory=rest_client)

    smelter: Smelter = Smelter(
        overseer=overseer,
        general=config.general,
        default_schema=config.default_schema,
        storage=config.storage,
    )

    crucible: Crucible = Crucible(overseer=overseer, storage=config.storage)

    forge: Forge = Forge(rest_factory=rest_client)

    create_project_example(smelter=smelter, crucible=crucible, forge=forge)
    create_experiment_example(smelter, crucible, forge)
    create_dataset_example(smelter, crucible, forge)
    create_datafile_example(smelter, crucible, forge)

    print("Done")
