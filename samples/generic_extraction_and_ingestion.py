# pylint: disable-all
# noqa
# nosec
# type: ignore

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

-Ingestion Factory
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

from src import profiles
from src.beneficiations.beneficiation import Beneficiation
from src.beneficiations.parsers.json_parser import JsonParser
from src.config.config import ConfigFromEnv
from src.conveyor.conveyor import Conveyor
from src.conveyor.transports.rsync import RsyncTransport
from src.crucible import crucible
from src.extraction.extraction_plant import ExtractionPlant
from src.forges import forge
from src.ingestion_factory.factory import IngestionFactory
from src.miners.miner import Miner
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.overseers.overseer import Overseer
from src.profiles.profile_loader import ProfileLoader
from src.prospectors.prospector import Prospector
from src.smelters import smelter

# ---Constants
logger = logging.getLogger(__name__)
config = ConfigFromEnv()


# ---Code
def main():
    """Extracts and ingests metadata from dataclasses into MyTardis.

    The extraction is done using the ExtractionPlant class that runs the prospector, miner, and beneficiation

    The ingestion is done using the IngestionFactory class that runs the smelter, crucible, and forge
    """

    ###Extraction step
    pth = Path("Replace/This/With/Your/Ingestion/Path/Here")
    profile = "replace_this_with_your_profile_name"
    profile_loader = ProfileLoader(profile)

    prospector = Prospector(profile_loader.load_custom_prospector)
    miner = Miner(profile_loader.load_custom_miner)

    beneficiation = Beneficiation(profile_loader.load_custom_beneficiation)

    ext_plant = ExtractionPlant(prospector, miner, beneficiation)
    ingestible_dataclasses = ext_plant.extract(pth)
    #  ingestibles = ext_plant.run_extraction(pth, bc.JSON_FORMAT) #for json files

    # Start conveyor to transfer datafiles.
    datafiles = ingestible_dataclasses.get_datafiles()
    rsync_transport = RsyncTransport(Path("/replace/with/your/transfer/destination"))
    conveyor = Conveyor(rsync_transport)
    conveyor_process = conveyor.initiate_transfer(pth, datafiles)

    ###Ingestion step
    mt_rest = MyTardisRESTFactory(config.auth, config.connection)
    overseer = Overseer(mt_rest)
    # TODO YJ complete the rest of the template script here once the ingestion factory is renovated

    # After metadata ingestion is done, wait for conveyor to finish.
    conveyor_process.join()


if __name__ == "__main__":
    main()
