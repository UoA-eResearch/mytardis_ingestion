# ---Imports
import logging
import subprocess
from pathlib import Path

from src import profiles
from src.beneficiations.beneficiation import Beneficiation
from src.config.config import ConfigFromEnv
from src.crucible.crucible import Crucible
from src.extraction_plant.extraction_plant import ExtractionPlant
from src.forges import forge
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

pth = "tests/fixtures/fixtures_example.yaml"
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

crucible = Crucible(overseer)

smelter = Smelter(
    general = config.general,
    default_schema = config.default_schema,
    storage = config.storage,
    overseer= overseer,
)

factory = IngestionFactory(
    config = config,
    mt_rest = mt_rest,
    overseer = overseer,
    smelter = smelter,
)

rawprojects = ingestible_dataclasses.projects
rawexperiments = ingestible_dataclasses.experiments
rawdatasets = ingestible_dataclasses.datasets
datafiles = ingestible_dataclasses.datafiles
factory.ingest_projects(ingestible_dataclasses.projects)