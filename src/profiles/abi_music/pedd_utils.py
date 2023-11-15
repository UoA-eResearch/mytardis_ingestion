"""
Helpers for assembling the PEDD dataclasses from data in the filesystem
"""
import hashlib
import mimetypes
from pathlib import Path

from src.blueprints.datafile import RawDatafile


def compute_md5(file: Path) -> str:
    """
    Compute the MD5 hash of the file referenced by `file`.
    NOTE: a version already exists in datafile_metadata_helpers.py - should unify them
    """
    with file.open("rb") as f:
        file_hash = hashlib.md5()
        while chunk := f.read(8192):
            file_hash.update(chunk)

        return file_hash.hexdigest()


def collate_datafile_info(
    file_rel_path: Path, root_dir: Path, dataset_name: str
) -> RawDatafile:
    """
    Collect and collate all the information needed to define a datafile dataclass
    """
    full_path = root_dir / file_rel_path

    mimetype, _ = mimetypes.guess_type(full_path)
    if mimetype is None:
        mimetype = "application/octet-stream"

    return RawDatafile(
        filename=file_rel_path.name,
        directory=file_rel_path,
        md5sum=compute_md5(full_path),
        mimetype=mimetype,
        size=full_path.stat().st_size,
        users=None,
        groups=None,
        dataset=dataset_name,
        metadata=None,
        schema=None,
    )


def collect_datafiles(root: Path, dataset_name: str) -> list[RawDatafile]:
    """
    Find all files recursively under `root`, and produce a `RawDatafile` object
    for each one.
    """
    datafiles: list[RawDatafile] = []

    # TODO: collect file filtering logic in a filter object
    children = root.rglob("*")

    for entry in children:
        if not entry.is_file():
            continue

        # TODO: make a proper filter to ignore everything
        if entry.name.endswith(".DS_Store"):
            continue

        relative_path = entry.relative_to(root)

        datafile = collate_datafile_info(relative_path, root, dataset_name)
        datafiles.append(datafile)

    return datafiles


def main() -> None:
    """
    Entry point, just for testing until the full ingestion runner is created
    """
    test_path = Path(
        "/mnt/abi_test_data/Vault/Raw/BenP/PID143/BlockA/220504-123120/BlockA_561_000"
    )

    datafiles = collect_datafiles(test_path, "dummy_dataset")

    for file in datafiles[0:5]:
        print(file.model_dump_json(indent=4))


if __name__ == "__main__":
    main()
