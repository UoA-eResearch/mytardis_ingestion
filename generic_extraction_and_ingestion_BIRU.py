# pylint: disable=C0103
"""NB: THIS WILL NOT WORK AS IT IS AND IS PROVIDED FOR INDICATIVE PURPOSES ONLY

Script to create the objects in MyTardis.

The workflow for these scripts are as follows:
-Extraction Plant
 -- Prospector: involves checking the files and metadata to
    see which files are worthy/unworthy of ingestion into MyTardis. Please
    note that this is not compulsory if the IME is used.
 -- Miner: involves converting and standardising the metadata so
    that it can be beneficiated. Please note that this is not compulsory if the
    IME is used.
 -- Beneficiating the metadata: involves parsing the mined or IME-produced
    metadata into raw dataclasses.

-Conveyor
   Taking datafile metadata from the extraction plant, we generate a list of
   of files to transfer into MyTardis, then move them using a configured
   Transport. This happens in a parallel process to the ingestion factory.

-Ingestion Factory # Not working as intended yet, currently using the components separately
 -- Smelting: Refines the raw dataclasses into refined dataclasses which closely
    follows the MyTardis's data formats.
 -- Crucible: Swaps out directory paths in the metadata into URI's. Used mainly
    for locating datafiles in MyTardis's object store.
 -- Forges: Creates MyTardis objects, from the URI-converted dataclasses,
    in the MyTardis database.

For the extraction plant processes, there are two pathways that depends on
whether the IME is used. The rest of the steps remain the same from
beneficiation onwards.
"""
# ---Imports
import logging
from pathlib import Path

from src.beneficiations.beneficiation import Beneficiation
from src.config.config import ConfigFromEnv
from src.crucible.crucible import Crucible
from src.extraction_plant.extraction_plant import ExtractionPlant
from src.forges.forge import Forge
from src.helpers.mt_rest import MyTardisRESTFactory
from src.miners.miner import Miner
from src.overseers.overseer import Overseer
from src.profiles.profile_loader import ProfileLoader
from src.prospectors.prospector import Prospector
from src.smelters.smelter import Smelter

# ---Constants
logger = logging.getLogger(__name__)
config = ConfigFromEnv()

# pth = "tests/fixtures/fixtures_example.yaml"
pth = (
    "/Volumes/resmed202000005-biru-shared-drive/"
    "MyTardisTestData/Haruna_HierarchyStructure/ingestion.yaml"
)
profile = str(Path("idw"))
profile_loader = ProfileLoader(profile)

prospector = Prospector(profile_loader.load_custom_prospector())
miner = Miner(profile_loader.load_custom_miner())

beneficiation = Beneficiation(profile_loader.load_custom_beneficiation())

ext_plant = ExtractionPlant(prospector, miner, beneficiation)
ingestible_dataclasses = ext_plant.run_extraction(pth)

###Ingestion step
mt_rest = MyTardisRESTFactory(config.auth, config.connection)
overseer = Overseer(mt_rest)

crucible = Crucible(
    overseer=overseer,
    storage=config.storage,
)

smelter = Smelter(
    general=config.general,
    default_schema=config.default_schema,
    storage=config.storage,
    overseer=overseer,
)

forge = Forge(mt_rest)
# default_schema = config.default_schema
# schema_project = default_schema.project
# print(schema_project)
prj = ingestible_dataclasses.projects[0]
exp = ingestible_dataclasses.experiments[0]
ds = ingestible_dataclasses.datasets[0]
df = ingestible_dataclasses.datafiles[0]
# print(prj)
# print(exp)
# print(ds)
for project_obj in ingestible_dataclasses.projects:
    refined_project, refined_project_parameters = smelter.smelt_project(project_obj)
    prepared_project = crucible.prepare_project(refined_project)
    forge = Forge(mt_rest)
    forge.forge_project(prepared_project, refined_project_parameters)

for experiment_obj in ingestible_dataclasses.experiments:
    refined_experiment, refined_experiment_parameters = smelter.smelt_experiment(
        experiment_obj
    )
    prepared_experiment = crucible.prepare_experiment(refined_experiment)
    forge.forge_experiment(prepared_experiment, refined_experiment_parameters)

for dataset_obj in ingestible_dataclasses.datasets:
    refined_dataset, refined_dataset_parameters = smelter.smelt_dataset(dataset_obj)
    prepared_dataset = crucible.prepare_dataset(refined_dataset)
    forge.forge_dataset(prepared_dataset, refined_dataset_parameters)

for datafile_obj in ingestible_dataclasses.datafiles:
    smelted_datafile = smelter.smelt_datafile(datafile_obj)
    refined_datafile = smelted_datafile
    try:
        prepared_datafile = crucible.prepare_datafile(refined_datafile)
        forge.forge_datafile(prepared_datafile)
    except AttributeError:
        print(datafile_obj)
