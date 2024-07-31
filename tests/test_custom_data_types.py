# pylint: disable=missing-function-docstring,missing-module-docstring
# pylint: disable=missing-class-docstring
# nosec assert_used
# flake8: noqa S101

import pytest
from pydantic import BaseModel

from src.blueprints.custom_data_types import Username


class DummyUsernames(BaseModel):
    user: Username


@pytest.mark.parametrize("upis", ["test001", "ts001", "tst001"])
def test_UPI_is_good(upis: Username) -> None:  # pylint: disable=invalid-name
    test_class = DummyUsernames(user=upis)
    assert test_class.user == upis


def test_UPI_wrong_type() -> None:  # pylint: disable=invalid-name
    bad_upi = True
    with pytest.raises(ValueError) as e_info:
        _ = DummyUsernames(user=bad_upi)
    assert (
        "1 validation error for DummyUsernames\nuser\n  Input should be a valid string "
        "[type=string_type, input_value=True, input_type=bool]\n    "
    ) in str(e_info.value)


@pytest.mark.parametrize(
    "upis", ["totally_wrong", "t001", "tests001", "test01", "test0001"]
)
def test_malformed_UPI(upis: Username) -> None:  # pylint: disable=invalid-name
    with pytest.raises(ValueError) as e_info:
        _ = DummyUsernames(user=upis)
    assert (
        (
            "1 validation error for DummyUsernames\nuser\n  "
            f'Value error, Passed string value "{upis}" '
            f"is not a well formatted Username [type=value_error, input_value='{upis}', "
            "input_type=str]\n    "
        )
    ) in str(e_info.value)
