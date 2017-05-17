import json
import logging

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from search.search_client import SearchClient


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseConnector(object):
    def __init__(self):
        self.topic = ''
        self.index = ''
        self.doc_type = ''
        self.indexed_fields = {}

    def listen(self):
        try:
            logger.info('Listening on topic: {}'.format(self.topic))
            consumer = KafkaConsumer(self.topic)

            for msg in consumer:
                object_dict = self.extract_updated_object(msg)
                if object_dict:
                    updated_dict = self.get_updated_indexed_fields(object_dict)
                    if updated_dict:
                        try:
                            self.update_index(updated_dict)
                        except Exception as se:
                            error_message = ('Error with SearchClient: {}\n'
                                             'Failed attempt to update with doc: {}'
                                             ).format(se, updated_dict)
                            logger.error(error_message)

        except Exception as ex:
            if isinstance(ex, KafkaError):
                logger.error('Error with Kafka: {}'.format(ex))

    def extract_updated_object(self, msg):
        value = getattr(msg, 'value', None)
        if not value:
            return None
        str_value = str(msg.value, 'utf-8')
        payload = json.loads(str_value)['payload']
        object_dict = payload['after']
        return object_dict

    def get_updated_indexed_fields(self, object_dict):
        updated_indexed_fields = {}
        for key in object_dict:
            if key in self.indexed_fields:
                updated_indexed_fields[key] = object_dict[key]
        return updated_indexed_fields

    def update_index(self, object_dict):
        search_client = SearchClient()
        search_client.update(self.index, self.doc_type, object_dict['id'], object_dict)
