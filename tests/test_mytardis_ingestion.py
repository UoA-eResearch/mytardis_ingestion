# pylint: disable=missing-module-docstring,missing-function-docstring
# nosec assert_used

from src import __version__


def test_version() -> None:
    assert __version__ == "0.8.0"
