"""Utility functions for validating data."""


def is_hex(value: str) -> bool:
    """Check if a string is a valid hexadecimal string."""
    try:
        _ = int(value, 16)
    except ValueError:
        return False

    return True
