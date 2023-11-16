# pylint: disable=missing-docstring
from pathlib import Path

from src.utils.filesystem.filters import (
    ExtensionPrefixFilter,
    FileExclusionPatterns,
    NameSuffixFilter,
    PathFilterSet,
    PathPatternFilter,
)


def test_name_suffix_filter() -> None:
    filt = NameSuffixFilter([".bad", ".double.bad", "._.bad.dots."])

    assert filt.exclude(Path("path/to/file.bad"))
    assert filt.exclude(Path("path/to/file.double.bad"))
    assert filt.exclude(Path("path/to/file._.bad.dots."))

    assert not filt.exclude(Path("path/to/file.bad.txt"))
    assert not filt.exclude(Path("path/to/file.double.bad.txt"))
    assert not filt.exclude(Path("path/to/file._.bad.dots.txt"))


def test_extension_prefix_filter() -> None:
    filt = ExtensionPrefixFilter(["._", ".bad"])

    assert filt.exclude(Path("path/to/file._bad"))
    assert filt.exclude(Path("path/to/file.badfile"))

    assert not filt.exclude(Path("path/to/file._.bad.dots."))
    assert not filt.exclude(Path("path/to/file._bad.double"))
    assert not filt.exclude(Path("path/to/file.bad.double"))


def test_path_pattern_filter() -> None:
    patterns = FileExclusionPatterns(suffixes=[".bad"], extension_prefixes=["._"])
    filt = PathPatternFilter(patterns)

    assert filt.exclude(Path("file.bad"))
    assert filt.exclude(Path("path/to/file.bad"))
    assert filt.exclude(Path("path/to/file._bad"))

    assert not filt.exclude(Path("path/to/file.badfile"))
    assert not filt.exclude(Path("path/to/file._.txt"))


def test_file_system_filter_set() -> None:
    filt = PathFilterSet(filter_system_files=True)

    assert filt.exclude(Path("path/to/file.DS_Store"))
    assert filt.exclude(Path("path/to/file.thumbs.db"))

    assert not filt.exclude(Path("path/to/file.txt"))

    filt = PathFilterSet(filter_system_files=False)

    assert not filt.exclude(Path("path/to/file.DS_Store"))
    assert not filt.exclude(Path("path/to/file.thumbs.db"))

    assert not filt.exclude(Path("path/to/file.txt"))

    filt.add(lambda p: p.name == "file.txt")

    assert filt.exclude(Path("path/to/file.txt"))
