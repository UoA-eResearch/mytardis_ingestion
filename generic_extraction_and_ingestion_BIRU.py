# ---Imports
import logging
import subprocess
from pathlib import Path

from src import profiles
from src.beneficiations.beneficiation import Beneficiation
from src.config.config import ConfigFromEnv
from src.crucible.crucible import Crucible
from src.extraction_plant.extraction_plant import ExtractionPlant
from src.forges.forge import Forge
from src.helpers.mt_rest import MyTardisRESTFactory
from src.ingestion_factory.factory import IngestionFactory
from src.miners.miner import Miner
from src.overseers.overseer import Overseer
from src.profiles.idw.custom_beneficiation import CustomBeneficiation
from src.profiles.profile_loader import ProfileLoader
from src.prospectors.prospector import Prospector
from src.smelters.smelter import Smelter

# ---Constants
logger = logging.getLogger(__name__)
config = ConfigFromEnv()

#pth = "tests/fixtures/fixtures_example.yaml"
pth = '/Volumes/resmed202000005-biru-shared-drive/MyTardisTestData/Haruna_HierarchyStructure/ingestion.yaml'
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
#print(prj)
#print(exp)
#print(ds)
'''for project_obj in ingestible_dataclasses.projects:
    refined_project, refined_project_parameters = smelter.smelt_project(project_obj)
    prepared_project = crucible.prepare_project(refined_project)
    forge = Forge(mt_rest)
    forge.forge_project(prepared_project, refined_project_parameters)

for experiment_obj in ingestible_dataclasses.experiments:
    refined_experiment, refined_experiment_parameters = smelter.smelt_experiment(experiment_obj)
    prepared_experiment = crucible.prepare_experiment(refined_experiment)
    forge.forge_experiment(prepared_experiment, refined_experiment_parameters)

for dataset_obj in ingestible_dataclasses.datasets:
    refined_dataset, refined_dataset_parameters = smelter.smelt_dataset(dataset_obj)
    prepared_dataset = crucible.prepare_dataset(refined_dataset)
    forge.forge_dataset(prepared_dataset, refined_dataset_parameters)'''

for datafile_obj in ingestible_dataclasses.datafiles:
    smelted_datafile = smelter.smelt_datafile(datafile_obj)
    refined_datafile = smelted_datafile
    try:
        prepared_datafile = crucible.prepare_datafile(refined_datafile)
        forge.forge_datafile(prepared_datafile)
    except AttributeError:
        print(datafile_obj)
    
'''smelted_datafile = smelter.smelt_datafile(df)
refined_datafile = smelted_datafile
prepared_datafile = crucible.prepare_datafile(refined_datafile)
forge.forge_datafile(prepared_datafile)'''

'''test_project = ingestible_dataclasses.projects[0]
test_experiment = ingestible_dataclasses.experiments[0]
test_dataset = ingestible_dataclasses.datasets[0]
test_datafiles = ingestible_dataclasses.datafiles[0:1]

test_project.identifiers = ["testp1"]
test_experiment.identifiers = ["teste1"]
test_dataset.identifiers = ["testd1"]
test_experiment.projects = ["testP1"]
test_dataset.experiments = ["testE1"]
test_dataset.instrument = "Olympus FV1000"


refined_project, refined_project_parameters = smelter.smelt_project(test_project)
prepared_project = crucible.prepare_project(refined_project)
forge = Forge(mt_rest)
forge.forge_project(prepared_project, refined_project_parameters)

smelted_experiment = smelter.smelt_experiment(test_experiment)
refined_experiment, refined_experiment_parameters = smelted_experiment
prepared_experiment = crucible.prepare_experiment(refined_experiment)
forge.forge_experiment(prepared_experiment, refined_experiment_parameters)

smelted_dataset = smelter.smelt_dataset(test_dataset)
refined_dataset, refined_dataset_parameters = smelted_dataset
prepared_dataset = crucible.prepare_dataset(refined_dataset)
forge.forge_dataset(prepared_dataset, refined_dataset_parameters)

for df in test_datafiles:
    smelted_datafile = smelter.smelt_datafile(df)
    refined_datafile = smelted_datafile
    prepared_datafile = crucible.prepare_datafile(refined_datafile)
    forge.forge_datafile(prepared_datafile)'''

# refined_datafiles, refined_datafiles_parameters = smelter.smelt_datafile(test_datafile)
# prepared_datafiles = crucible.prepare_datafile(refined_datafiles)
# 3forge.forge_datafile(prepared_datafiles, refined_datafiles_parameters)

# factory = IngestionFactory(
#    config = config,
#    mt_rest = mt_rest,
#    overseer = overseer,
#    smelter = smelter,
#    crucible= crucible,
# )

# rawprojects = ingestible_dataclasses.projects
# rawexperiments = ingestible_dataclasses.experiments
# rawdatasets = ingestible_dataclasses.datasets
# datafiles = ingestible_dataclasses.datafiles
# print(rawprojects)

# factory.ingest_projects(ingestible_dataclasses.projects)

# factory.ingest_experiments(ingestible_dataclasses.experiments)
# factory.ingest_datasets(ingestible_dataclasses.datasets)
# factory.ingest_datafiles(ingestible_dataclasses.datafiles)
