from abc import ABC, abstractmethod


class DAGConfigurator(ABC):
    """Abstract base class for DAG configurators."""

    @abstractmethod
    def create_dag(self):
        """Create the DAG using the specified configuration."""

    @abstractmethod
    def add_task(self, task):
        """add tasks to the DAG ."""
