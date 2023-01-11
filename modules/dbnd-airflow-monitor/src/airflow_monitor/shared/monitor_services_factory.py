# © Copyright Databand.ai, an IBM Company 2022
from abc import ABC, abstractmethod


class MonitorServicesFactory(ABC):
    """
    This class is a factory that returns all the required components for running a monitor
    Inherit from this class, implement the methods and pass it to a MultiServerMonitor instance.
    """

    @abstractmethod
    def get_components_dict(self):
        pass

    @abstractmethod
    def get_data_fetcher(self, server_config):
        pass

    @abstractmethod
    def get_syncer_management_service(self):
        pass

    @abstractmethod
    def get_tracking_service(self, server_config):
        pass

    def get_components(self, server_config, syncer_management_service):
        tracking_service = self.get_tracking_service(server_config)
        data_fetcher = self.get_data_fetcher(server_config)
        components_dict = self.get_components_dict()

        all_components = []
        for _, syncer_class in components_dict.items():
            syncer_instance = syncer_class(
                config=server_config,
                tracking_service=tracking_service,
                syncer_management_service=syncer_management_service,
                data_fetcher=data_fetcher,
            )
            all_components.append(syncer_instance)

        return all_components
