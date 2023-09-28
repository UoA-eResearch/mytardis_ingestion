import logging
import mimetypes

from src.blueprints.common_models import Parameter, ParameterSet
from src.blueprints.custom_data_types import URI
from src.blueprints.datafile import Datafile, DatafileReplica
from src.blueprints.dataset import Dataset, RawDataset, RefinedDataset
from src.blueprints.experiment import Experiment, RawExperiment, RefinedExperiment
from src.blueprints.project import (
    Project,
    ProjectFileSystemStorageBox,
    RawProject,
    RefinedProject,
)
from src.config.config import ConfigFromEnv, StorageBoxConfig
from src.crucible.crucible import Crucible
from src.forges.forge import Forge
from src.helpers.enumerators import DataClassification
from src.helpers.mt_rest import MyTardisRESTFactory
from src.overseers.overseer import Overseer
from src.smelters.smelter import Smelter

logging.basicConfig(
    filename="upload_example.log",
    format="%(levelname)s:%(message)s",
    level=logging.DEBUG,
)


#####################################################################################


def print_field_comparison(models):
    jsons = {name: model.model_dump() for name, model in models.items()}

    fields = set()
    for json in jsons.values():
        for key in json.keys():
            fields.add(key)

    ordered_fields = sorted(list(fields))

    model_names = [name for name in models.keys()]
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
            # max_length = 40
            display = entry[0:display_limit].ljust(display_limit)
            print(display, " | ", end="")
        # print('|')
        print("")


#####################################################################################
# PROJECT


def create_project_example(smelter: Smelter, crucible: Crucible, forge: Forge) -> None:
    raw_project: RawProject = RawProject(
        name="Andrew W SCF Test 1",
        description="A test project for working through a smelter-crucible-forge example",
        principal_investigator="awil308",
        data_classification=DataClassification.PUBLIC,
        created_by=None,
        url=None,
        users=None,
        groups=None,
        identifiers=["andrew_scf_example"],
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

    if smelt_result is None:
        raise Exception("Something went wrong during smelting")

    refined_project, parameters = smelt_result

    # print('Refined Project:\n', refined_project.model_dump_json(indent=3))
    # print('Parameters\n:', parameters.model_dump_json(indent=3) if parameters else 'None')

    project: Project = crucible.prepare_project(refined_project)
    assert project is not None, "Crucible stage failed"

    uri, param_uri = forge.forge_project(
        refined_object=project, project_parameters=None
    )
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

# def forge_experiment_example(forge : Forge):

#     experiment : Experiment = Experiment(
#         title="Andrew Test Experiment 4",
#         description="A dummy experiment for trying out the API",
#         data_classification=DataClassification.PUBLIC,
#         created_by='awil308',
#         url=None,
#         locked=False,
#         users=None,
#         groups=None,
#         identifiers=None,
#         projects=['api/v1/project/13/'],
#         institution_name="University of Auckland",
#         start_time=None,
#         end_time=None,
#         created_time=None,
#         update_time=None,
#         embargo_until=None
#     )

#     uri, param_uri = forge.forge_experiment(refined_object=experiment, experiment_parameters=None)

#     print('Forged experiment:')
#     print('URI: ', uri)
#     print('Parameter URI: ', param_uri)


def create_experiment_example(smelter: Smelter, crucible: Crucible, forge: Forge):
    raw_experiment = RawExperiment(
        title="Andrew Test Experiment 5",
        description="A dummy experiment for trying out the API",
        data_classification=DataClassification.PUBLIC,
        created_by="awil308",
        url=None,
        locked=False,
        users=None,
        groups=None,
        identifiers=[],
        # projects=['/api/v1/project/36/'],
        projects=["Andrew W SCF Test 1"],
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

    experiment: Experiment = crucible.prepare_experiment(refined_experiment)
    assert experiment is not None, "Experiment creation failed"

    uri, param_uri = forge.forge_experiment(
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

# def forge_dataset_example(forge : Forge, with_metadata : bool = False):

#     parameter_set : ParameterSet = ParameterSet(
#         schema='/api/v1/schema/7/',
#         # schema='http://andrew-test.com/df-param-schema/1',
#         parameters=[
#             Parameter(name='image_width', value=1921),
#             Parameter(name='image_height', value=1082),
#             Parameter(name='num_channels', value=4),
#         ]
#     )

#     dataset : Dataset = Dataset(
#         description="Andrew's test dataset 4 with added metadata",
#         data_classification=DataClassification.PUBLIC,
#         directory=None,   # TODO: what's this for?
#         users=None,
#         groups=None,
#         immutable=False,
#         identifiers=None,
#         experiments=['/api/v1/experiment/5/'],
#         instrument='/api/v1/instrument/1/',
#         created_time=None,
#         modified_time=None
#     )

#     uri, param_uri = forge.forge_dataset(
#         refined_object=dataset,
#         dataset_parameters=parameter_set if with_metadata else None
#     )

#     print('Forged dataset:')
#     print('URI: ', uri)
#     print('Parameter URI: ', param_uri)


def create_dataset_example(smelter: Smelter, crucible: Crucible, forge: Forge):
    raw_dataset = RawDataset(
        description="Andrew's test dataset 5 with added metadata",
        data_classification=DataClassification.PUBLIC,
        directory=None,
        users=None,
        groups=None,
        immutable=False,
        identifiers=None,
        experiments=["/api/v1/experiment/17/"],
        instrument="/api/v1/instrument/1/",
        metadata={"image_width": 1921, "image_height": 1082, "num_channels": 4},
        schema="http://andrew-test.com/df-param-schema/1/",
        created_time=None,
        modified_time=None,
    )

    smelt_result = smelter.smelt_dataset(raw_dataset)
    assert smelt_result is not None, "Smelting dataset failed"

    refined_dataset, parameters = smelt_result

    dataset: Dataset = crucible.prepare_dataset(refined_dataset)
    assert dataset is not None, "Dataset prepatation failed"

    uri, param_uri = forge.forge_dataset(
        refined_object=dataset, dataset_parameters=parameters
    )
    assert uri is not None, "Failed to forge dataset"
    print("Forged dataset with URI: ", uri, ", and parameters: ", parameters)

    print_field_comparison(
        {
            "RawDataset": raw_dataset,
            "RefinedDataset": refined_dataset,
            "Dataset": dataset,
        }
    )


#####################################################################################
# DATAFILE


def forge_datafile_example(forge: Forge, with_metadata: bool = False):
    parameter_set: ParameterSet = ParameterSet(
        schema=URI("/api/v1/schema/7/"),
        # schema='http://andrew-test.com/df-param-schema/1',
        parameters=[
            Parameter(name="image_width", value=1920),
            Parameter(name="image_height", value=1080),
            Parameter(name="num_channels", value=3),
        ],
    )

    datafile: Datafile = Datafile(
        filename="dummy_image_2.png",
        directory="a/dummy/dir/dummy_image_2.png",
        md5sum="a345bcf3489e8dd8e8a823b01cc834f2",
        mimetype="image/png",
        size="3474853",
        users=None,
        groups=None,
        replicas=[
            DatafileReplica(
                uri="a/dummy/dir/dummy_image_2.png", location="andrew_w_test"
            )
        ],
        parameter_sets=[parameter_set] if with_metadata else None,
        dataset="/api/v1/dataset/1/",
    )

    _ = forge.forge_datafile(refined_object=datafile)

    print("Forged datafile")


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

    # forge_experiment_example(forge)
    # forge_dataset_example(forge, with_metadata=True)
    # forge_datafile_example(forge, with_metadata=True)

    # create_project_example(smelter=smelter, crucible=crucible, forge=forge)
    # create_experiment_example(smelter, crucible, forge)
    create_dataset_example(smelter, crucible, forge)

    print("Done")
