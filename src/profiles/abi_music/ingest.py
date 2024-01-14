"""
Ingestion runner for the ABI MuSIC data
"""

import asyncio
import io
import logging
from pathlib import Path

import trio
import typer

from src.config.config import ConfigFromEnv
from src.ingestion_factory.factory import IngestionFactory
from src.profiles.abi_music import parsing
from src.utils import log_utils
from src.utils.filesystem.filesystem_nodes import DirectoryNode
from src.utils.timing import Timer


async def main2(data_root: Path, log_file: Path = Path("abi_ingestion.log")) -> None:
    """
    Run an ingestion for the ABI MuSIC data
    """
    log_utils.init_logging(file_name=str(log_file), level=logging.DEBUG)
    config = ConfigFromEnv()
    timer = Timer(start=True)

    root_dir = DirectoryNode(data_root)
    if root_dir.empty():
        raise ValueError("Data root directory is empty. May not be mounted.")

    dataclasses = parsing.parse_data(root_dir)

    logging.info("Number of datafiles: %d", len(dataclasses.get_datafiles()))

    # Does this logging still meet our needs?
    stream = io.StringIO()
    dataclasses.print(stream)
    logging.info(stream.getvalue())

    elapsed = timer.stop()
    logging.info("Finished parsing data directory into PEDD hierarchy")
    logging.info("Total time (s): %.2f", elapsed)

    logging.info("Submitting to MyTardis")
    timer.start()

    ingestion_agent = IngestionFactory(config=config)

    await ingestion_agent.ingest(
        dataclasses.get_projects(),
        dataclasses.get_experiments(),
        dataclasses.get_datasets(),
        dataclasses.get_datafiles(),
    )

    elapsed = timer.stop()
    logging.info("Finished submitting dataclasses to MyTardis")
    logging.info("Total time (s): %.2f", elapsed)


def main1(data_root: Path, log_file: Path = Path("abi_ingestion.log")) -> None:
    # trio.run(main2, data_root, log_file)
    asyncio.run(main2(data_root, log_file))


if __name__ == "__main__":
    typer.run(main1)
