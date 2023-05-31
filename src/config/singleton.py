"""Create a Singleton metaclass.
"""

from typing import Any, Dict, List


class Singleton(type):
    """Meta-class to make classes singletons as needed."""

    _instances: Dict[Any, Any] = {}

    def __call__(
        cls: Any,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ) -> Any:  # sourcery skip: instance-method-first-arg-name
        """Check if a copy of the object already exists and return it if it does."""
        if cls not in cls._instances:
            cls._instances[cls] = super(
                Singleton,
                cls,
            ).__call__(
                *args,
                **kwargs,
            )
        return cls._instances[cls]

    def clear(cls: Any) -> None:  # sourcery skip: instance-method-first-arg-name
        """Clear the singleton so a new instance can be instatiated."""
        cls._instances = {}
