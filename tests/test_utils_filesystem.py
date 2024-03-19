# pylint: disable=missing-docstring
from pathlib import Path

from src.utils.filesystem.filters import (
    FileExclusionPatterns,
    NamePrefixFilter,
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
    filt = NamePrefixFilter(["._", ".bad"])

    assert filt.exclude(Path("path/to/._badfile.txt"))
    assert filt.exclude(Path("path/to/._.txt"))
    assert filt.exclude(Path("path/to/.badfile.txt"))

    assert not filt.exclude(Path("path/to/file._.bad.dots."))
    assert not filt.exclude(Path("path/to/file._bad._double"))
    assert not filt.exclude(Path("path/to/file.bad._double"))


def test_path_pattern_filter() -> None:
    patterns = FileExclusionPatterns(suffixes=[".bad"], name_prefixes=["._"])
    filt = PathPatternFilter(patterns)

    assert filt.exclude(Path("file.bad"))
    assert filt.exclude(Path("path/to/file.bad"))
    assert filt.exclude(Path("path/to/._file.txt"))

    assert not filt.exclude(Path("path/to/file.badfile"))
    assert not filt.exclude(Path("path/to/file._.txt"))
    assert not filt.exclude(Path("path/to/file._txt"))


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
