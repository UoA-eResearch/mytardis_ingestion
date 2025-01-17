"""
CLI frontend for extracting metadata, and ingesting it with the data into MyTardis.
"""

import logging

import typer

from src.cli.cmd_clean import clean
from src.cli.cmd_ingest import extract, ingest, upload
from src.cli.cmd_report import report

app = typer.Typer()
logger = logging.getLogger(__name__)

# =============================================================================
# COMMANDS
# =============================================================================

# Add ingest command, for extracting and uploading metadata and files.
app.command()(ingest)
# Add extract command, for extracting metadata and serialising to file.
app.command()(extract)
# Add upload command, for uploading serialised, extracted metadata.
app.command()(upload)
# Add report command, for generating a report on the status of datafiles.
app.command()(report)
# Add clean command, for deleting datafiles that have been successfully ingested and verified.
app.command()(clean)

if __name__ == "__main__":
    app()
