# pylint: disable=missing-function-docstring,W0621
"""Tests to validate the common_file_checks functions"""

from pathlib import Path
from typing import Any, List

import mock
from pytest import fixture

import src.utils.filesystem.filters as fs_filters
from src.prospectors.common_file_checks import (
    check_file_prefix,
    iterate_dir,
    perform_common_file_checks,
)

COMMON_MACOS_SYS_FILES = fs_filters.MACOS_EXCLUSION_PATTERNS.suffixes or []
MACOS_PREFIXES_TO_REJECT = fs_filters.MACOS_EXCLUSION_PATTERNS.extension_prefixes or []
COMMON_WIN_SYS_FILES = fs_filters.WINDOWS_EXCLUSION_PATTERNS.suffixes or []


@fixture
def mock_path_glob_non_recursive() -> List[Path]:
    common_fnames = [*COMMON_MACOS_SYS_FILES, *COMMON_WIN_SYS_FILES]
    common_fpaths = [Path(item) for item in common_fnames]
    return [
        *common_fpaths,
        Path(".hidden"),
        Path("goodfile.test"),
        Path("goodfile2.test"),
        Path("._bad_prefix_file.test"),
        Path("bad_prefix_file.test"),
        Path("metadata_file_MyTardisMetadata.test"),
    ]


@fixture
def mock_rejection_list_non_recursive() -> List[Path]:
    common_fnames = [*COMMON_MACOS_SYS_FILES, *COMMON_WIN_SYS_FILES]
    common_fpaths = [Path(item) for item in common_fnames]
    return [
        *common_fpaths,
        Path("._bad_prefix_file.test"),
        Path("metadata_file_MyTardisMetadata.test"),
    ]


@fixture
def mock_ingestion_list_non_recursive() -> List[Path]:
    return [
        Path(".hidden"),
        Path("goodfile.test"),
        Path("goodfile2.test"),
        Path("bad_prefix_file.test"),
    ]


def test_check_file_prefix(mock_path_glob_non_recursive: List[Path]) -> None:
    assert (
        check_file_prefix(
            Path("._bad_prefix_file_2"),
            MACOS_PREFIXES_TO_REJECT,
            False,
            mock_path_glob_non_recursive,
        )
        is False
    )
    assert (
        check_file_prefix(
            Path("._bad_prefix_file.test"),
            MACOS_PREFIXES_TO_REJECT,
            False,
            mock_path_glob_non_recursive,
        )
        is True
    )
    assert (
        check_file_prefix(
            Path(".hidden.test"),
            MACOS_PREFIXES_TO_REJECT,
            False,
            mock_path_glob_non_recursive,
        )
        is False
    )
    assert (
        check_file_prefix(
            Path(".hidden.test"),
            MACOS_PREFIXES_TO_REJECT,
            True,
            mock_path_glob_non_recursive,
        )
        is True
    )


@mock.patch("pathlib.Path.glob")
@mock.patch("pathlib.Path.is_file")
def test_iterate_dir(
    mock_path_is_file: Any,
    mock_path_glob: Any,
    mock_path_glob_non_recursive: List[Path],
    mock_rejection_list_non_recursive: List[Path],
    mock_ingestion_list_non_recursive: List[Path],
) -> None:
    mock_path_glob.return_value = mock_path_glob_non_recursive
    mock_path_is_file.return_Value = True
    common_fnames = [*COMMON_MACOS_SYS_FILES, *COMMON_WIN_SYS_FILES]
    reject_hidden = False
    assert iterate_dir(
        Path("/this/does/nothing/due/to/mock"),
        common_fnames,
        MACOS_PREFIXES_TO_REJECT,
        reject_hidden,
        False,
    ) == (mock_rejection_list_non_recursive, mock_ingestion_list_non_recursive)
    rejection_list = mock_rejection_list_non_recursive
    ingestion_list = mock_ingestion_list_non_recursive
    rejection_list.append(Path(".hidden"))
    ingestion_list.remove(Path(".hidden"))
    reject_hidden = True
    test_rejection, test_ingestion = iterate_dir(
        Path("/this/does/nothing/due/to/mock"),
        common_fnames,
        MACOS_PREFIXES_TO_REJECT,
        reject_hidden,
        False,
    )
    assert sorted(test_rejection) == sorted(rejection_list)
    assert sorted(test_ingestion) == sorted(ingestion_list)


@mock.patch("pathlib.Path.glob")
@mock.patch("pathlib.Path.is_file")
def test_perform_common_file_checks(
    mock_path_is_file: Any,
    mock_path_glob: Any,
    mock_path_glob_non_recursive: List[Path],
    mock_rejection_list_non_recursive: List[Path],
    mock_ingestion_list_non_recursive: List[Path],
) -> None:
    mock_path_glob.return_value = mock_path_glob_non_recursive
    mock_path_is_file.return_Value = True
    assert perform_common_file_checks(
        Path("/this/does/nothing/due/to/mock"),
        False,
        False,
    ) == (mock_rejection_list_non_recursive, mock_ingestion_list_non_recursive)
