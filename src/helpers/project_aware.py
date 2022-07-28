"""Project specific decorators to reduce code reuse"""

import logging


def project_enabled(self) -> bool:
    """Test if projects are enabled and return the function if so,
    otherwise log a warning and return None"""
    if not self.projects_enabled:
        logger = logging.getLogger(__name__)
        logger.warning(
            (
                "MyTardis is not currently set up to use projects. Please check settings.py "
                "and ensure that the 'projects' app is enabled. This may require rerunning "
                "migrations."
            )
        )
        return False
    return True
