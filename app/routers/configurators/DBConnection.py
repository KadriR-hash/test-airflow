from abc import ABC, abstractmethod


class DBConnection(ABC):
    """Abstract base class for database configurators."""

    """def __init__(self, db_configurator):
        pass"""

    @abstractmethod
    def connect_to_db(self, db_configurator):
        """Configures the database using the specified configuration."""

    @abstractmethod
    def close(self, db_configurator):
        """Close the specified connection."""
