# © Copyright Databand.ai, an IBM Company 2022
from abc import ABC, abstractmethod


class MonitorServicesFactory(ABC):
    """
    This class is a factory that returns all the required components for running a monitor
    Inherit from this class, implement the methods and pass it to a BaseMultiServerMonitor instance.
    """

    @abstractmethod
    def get_data_fetcher(self, server_config):
        pass

    @abstractmethod
    def get_servers_configuration_service(self):
        pass

    @abstractmethod
    def get_tracking_service(self, tracking_source_uid):
        pass
