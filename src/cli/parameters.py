"""Shared parameter definitions for CLI."""

from pathlib import Path
from typing import Annotated, Optional, TypeAlias

import typer

SourceDataPathArg: TypeAlias = Annotated[
    Path,
    typer.Argument(
        help="Path to the source data (either a directory or file)",
        exists=True,
        dir_okay=True,
        file_okay=True,
    ),
]

ProfileNameOption: TypeAlias = Annotated[
    str,
    typer.Option(
        "--profile",
        "-p",
        help="Name of the ingestion profile to be used to extract the data",
    ),
]

ProfileVersionOption = Annotated[
    Optional[str],
    typer.Option(
        help="Version of the profile to be used. If left unspecified, the latest will be used",
    ),
]

LogFileOption: TypeAlias = Annotated[
    Path,
    typer.Option(help="Path to be used for the log file"),
]

StorageBoxOption: TypeAlias = Annotated[
    Optional[tuple[str, Path]],
    typer.Option(
        help="Name and filesystem directory of staging StorageBox to transfer datafiles into."
    ),
]
