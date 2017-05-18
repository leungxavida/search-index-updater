from connector.base_connector import BaseConnector
from search.index.updater.base_updater import BaseIndexUpdater


class BaseIndexAdapter(BaseConnector, BaseIndexUpdater):
    def listen_and_update_index(self):
        """
        Main method that combines listening and index updating.
        :return:
        """
        self.listen(self.handle_index_update)
