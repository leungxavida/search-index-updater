from elasticsearch import Elasticsearch


ELASTICSEARCH_SETTINGS = {
    'use_ssl': False,
    'host': 'localhost',
    'scheme': 'http',
    'port': 9200
}


class SearchClientError(Exception):
    def __init__(self, error_msg):
        self.error_msg = error_msg

    def __str__(self):
        return '<SearchClientError: {}>'.format(self.error_msg)


class SearchClient:
    class __SearchClient:
        def __init__(self):
            self.connection = Elasticsearch([ELASTICSEARCH_SETTINGS])

    instance = None

    def __init__(self):
        if not SearchClient.instance:
            SearchClient.instance = SearchClient.__SearchClient()

    def update(self, index, doc_type, object_id, updates_dict):
        try:
            update_body = {
                "doc": updates_dict,
                "doc_as_upsert": True
            }
            self.instance.connection.update(index, doc_type, object_id, update_body)
        except Exception as ex:
            raise SearchClientError(ex)
