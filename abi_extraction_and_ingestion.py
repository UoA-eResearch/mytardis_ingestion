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
import sys
from typing import Iterable, Optional

from src.config.config import ConfigFromEnv

from src.conveyor.transports.rsync import RsyncTransport
from src.helpers.mt_rest import MyTardisRESTFactory
from src.overseers.overseer import Overseer


def init_logging(file_name : Optional[str]):
   root = logging.getLogger()
   root.setLevel(logging.DEBUG)

   console_handler = logging.StreamHandler(sys.stdout)
   console_handler.setLevel(logging.DEBUG)
   console_handler.setFormatter(logging.Formatter("[%(levelname)s]: %(message)s"))
   root.addHandler(console_handler)
   
   if file_name:
      file_handler = logging.FileHandler(filename=file_name, mode="w")
      file_handler.setLevel(logging.DEBUG)
      file_handler.setFormatter(logging.Formatter("[%(levelname)s]: %(message)s"))
      root.addHandler(file_handler) 


config = ConfigFromEnv()


# def directories(path : Path) -> Iterable[Path]:
#     return (entry for entry in path.glob('*') if entry.is_dir())

def directories(path : Path) -> Iterable[Path]:
    for child in path.glob('*'):
        if child.is_dir():
            yield child

# ---Code
def main() -> None:
    """Extracts and ingests metadata from dataclasses into MyTardis.
   
    The extraction is done using the ExtractionPlant class that runs the prospector, miner, and beneficiation
   
    The ingestion is done using the IngestionFactory class that runs the smelter, crucible, and forge
    """

    init_logging("abi_extract_and_ingest.log")
    
    ###Extraction step
    data_root = Path("/mnt/abi_test_data")
    assert data_root.is_dir(), f'Data root {str(data_root)} is not a valid directory'

    # Basic sanity check
    assert any(path.is_dir() for path in data_root.glob('*')), 'Data root directory has no child directories. Does it need to be mounted?'
    
    # profile_name: str = 'abi_music'
    # profile_loader = ProfileLoader(profile_name)

    raw_dir = data_root / 'Vault' / 'Raw'
    zarr_dir = data_root / 'Zarr'
    process_dir = data_root / 'Process'

    assert raw_dir.is_dir(), 'Raw data directory not found'
    assert zarr_dir.is_dir(), 'Zarr directory not found'
    assert process_dir.is_dir(), 'Process directory not found'


    project_names = directories(raw_dir)


    # Within Raw:
    # - Raw should have projects (currently just "BenP")
    # - Project should have experiments (currently just BenP/PID143)
    # - Experiments should have datasets (currently BlockA, BlockB)

    # Within Zarr:
    # - There should be a folder for each dataset called "<project>-<experiment>-<dataset>"
    # - Within this is a directory ending with .zarr and a .json file. The name is a timestamp?
    # - Everything under these files should be datafiles
    # - Each dataset should have some metadata linking it to the corresponding "raw" dataset


    # Within Process:
    # - For each dataset, there should be a "<project>/<experiment>/<dataset file>", e.g. "BenP/PID143/BlockA"
    #   - This contains an intermediate directory named with a timestamp
    #   - Not sure if any other files in here need to be retained? Check with GS
    # - It's unclear whether "BlockA-Deconv" is something that should be retained or is easily regenerated. Need to check with GS.
    #











if __name__ == "__main__":
    main()



  #  prospector = CustomProspector()

    # prospector = profile_loader.load_custom_prospector()
   #  assert prospector is not None, 'Failed to load custom prospector'

    #  om_vault = prospector.prospect(path=str(data_root / 'Vault'), recursive=True)
    #  om_process = prospector.prospect(path=str(data_root / 'Process'), recursive=True)
    #  om_zarr = prospector.prospect(path=str(data_root / 'Zarr'), recursive=True)

   #  om_root = prospector.prospect(path=str(data_root), recursive=True)

    # TODO:  try running the miner
    #  miner = Miner(profile_loader.load_custom_miner)
   #  miner = CustomMiner()
    
   #  beneficiation = Beneficiation(profile_loader.load_custom_beneficiation)
    
   #  ext_plant = ExtractionPlant(prospector, miner, beneficiation)
   #  ingestible_dataclasses = ext_plant.run_extraction(data_root)
    #  ingestibles = ext_plant.run_extraction(pth, bc.JSON_FORMAT) #for json files
    
    # # Start conveyor to transfer datafiles.
   #  datafiles = ingestible_dataclasses.get_datafiles()
   #  rsync_transport = RsyncTransport(Path("/replace/with/your/transfer/destination"))
   #  conveyor = Conveyor(rsync_transport)
   #  conveyor_process = conveyor.initiate_transfer(data_root, datafiles)
    
    ###Ingestion step
   #  mt_rest = MyTardisRESTFactory(config.auth, config.connection)
   #  overseer = Overseer(mt_rest)
    # TODO YJ complete the rest of the template script here once the ingestion factory is renovated
    
    # After metadata ingestion is done, wait for conveyor to finish.
   #  conveyor_process.join()