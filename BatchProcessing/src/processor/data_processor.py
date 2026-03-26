

from abc import ABC, abstractmethod

""" DataProcessor is an abstract base class that defines the interface for processing data."""
class DataProcessor(ABC):

    @abstractmethod
    def process(self, data):
        pass
        