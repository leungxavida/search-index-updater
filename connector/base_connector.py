import json

from kafka import KafkaConsumer

from search.search_client import SearchClient


class BaseConnector(object):
    def __init__(self):
        self.topic = ''
        self.index = ''
        self.doc_type = ''
        self.indexed_fields = {}

    def listen(self):
        print("Listening on topic: {}".format(self.topic))
        consumer = KafkaConsumer(self.topic)

        for msg in consumer:
            object_dict = self.extract_updated_object(msg)
            updated_dict = self.get_updated_indexed_fields(object_dict)
            print('UPDATED CONTENT: {}'.format(updated_dict))
            if updated_dict:
                self.update_index(updated_dict)

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
        try:
            search_client = SearchClient()
            search_client.update(self.index, self.doc_type, object_dict['id'], object_dict)
        except Exception as e:
            print(e)
