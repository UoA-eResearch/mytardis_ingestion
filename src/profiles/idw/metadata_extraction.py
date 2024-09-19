"""
YAML parser module. This module is used for parsing YAML files into
appropriate dataclasses.
"""

import logging

# Standard library imports
from pathlib import Path

# Third-party imports
import yaml

# User-defined imports
from src.blueprints import RawDatafile, RawDataset, RawExperiment, RawProject
from src.extraction.manifest import IngestionManifest
from src.extraction.metadata_extractor import IMetadataExtractor
from src.profiles.idw.yaml_helper import YamlParser  # type: ignore[attr-defined]
from src.utils.filesystem.checksums import calculate_md5

# Constants
logger = logging.getLogger(__name__)

# Initialise the representers and constructors required for
# loading YAML elements.
YamlParser = YamlParser()


class IDWMetadataExtractor(IMetadataExtractor):
    """
    Extractor class for parsing IDW-specific YAML files into ingestible dataclasses.
    """

    # pylint: disable=arguments-renamed
    def extract(self, yaml_path: Path) -> IngestionManifest:
        """
        Parse a YAML file at the specified path and return a list of loaded objects.

        Args:
            fpath (str): The path to the YAML file.

        Returns:
            List[Union[RawDatafile, RawDataset, RawExperiment, RawProject]]: A list of loaded
            objects.
        """
        if not yaml_path.is_file() or yaml_path.suffix != ".yaml":
            raise ValueError(f"{yaml_path} is not a valid YAML file.")

        ingestible_dclasses = IngestionManifest(source_data_root=yaml_path.parent)
        logger.info("parsing %s", yaml_path)

        with open(yaml_path, encoding="utf-8") as f:
            data = yaml.safe_load_all(f)
            for obj in data:
                if isinstance(obj, RawProject):
                    ingestible_dclasses.add_project(obj)
                if isinstance(obj, RawExperiment):
                    ingestible_dclasses.add_experiment(obj)
                if isinstance(obj, RawDataset):
                    ingestible_dclasses.add_dataset(obj)
                if isinstance(obj, RawDatafile):
                    df_path = yaml_path.parent / obj.filepath
                    obj.md5sum = calculate_md5(df_path)
                    ingestible_dclasses.add_datafile(obj)
        return ingestible_dclasses
