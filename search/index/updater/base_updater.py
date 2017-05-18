import logging

from search.client.search_client import SearchClient


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseIndexUpdater(object):
    """
    Base Class for index updating.
    Used to specify the index, doc_type and indexed fields to be updated
    """
    def __init__(self):
        self.index = ''
        self.doc_type = ''
        self.indexed_fields = {}

    def handle_index_update(self, object_dict):
        """
        Extracts information from object dictionary and updates search index
        :param object_dict: dictionary data of object that has been updated
        :return:
        """
        updated_dict = self.get_updated_indexed_fields(object_dict)
        if updated_dict:
            try:
                self.update_index(updated_dict)
            except Exception as se:
                error_message = ('Error with SearchClient: {}\n'
                                 'Failed attempt to update with doc: {}'
                                 ).format(se, updated_dict)
                logger.error(error_message)

    def get_updated_indexed_fields(self, object_dict):
        """
        Extracts fields and returns object with only relevant index fields to update
        :param object_dict: dictionary containing all data of the object
        :return: dictionary containing only indexed fields as keys and their values
        """
        updated_indexed_fields = {}
        for key in object_dict:
            if key in self.indexed_fields:
                updated_indexed_fields[key] = object_dict[key]
        return updated_indexed_fields

    def update_index(self, object_dict):
        """
        Performs the index update
        :param object_dict: dictionary containing indexed fields as keys and their values
        :return:
        """
        search_client = SearchClient()
        search_client.update(self.index, self.doc_type, object_dict['id'], object_dict)
