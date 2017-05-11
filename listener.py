import json

from kafka import KafkaConsumer

from search.search_client import SearchClient


class UserIndex:
    topic = 'dbsvname.public.accounts_user'

    index = 'users'
    doc_type = 'user'

    indexed_fields = {'id', 'uuid', 'last_login', 'created', 'modified', 'first_name', 'last_name', 'is_active',
                      'is_staff', 'date_joined', 'timezone', 'subscription_status', 'zip_code', 'cancel_date'}


def main():
    indexed_object = UserIndex
    consumer = KafkaConsumer(indexed_object.topic)

    for msg in consumer:
        process_message(indexed_object, msg)


def process_message(indexed_object, msg):
    payload = json.loads(msg.value)['payload']
    op = payload['op']
    object_dict = payload['after']
    print object_dict
    updated_dict = _get_updated_indexed_fields(indexed_object, object_dict)
    update_index(indexed_object, updated_dict)


def _get_updated_indexed_fields(indexed_object, object_dict):
    updated_indexed_fields = {}
    for key in object_dict:
        if key in indexed_object.indexed_fields:
            updated_indexed_fields[key] = object_dict[key]
    return updated_indexed_fields


def update_index(indexed_object, object_dict):
    search_client = SearchClient()
    search_client.update(indexed_object.index, indexed_object.doc_type, object_dict['id'], object_dict)


if __name__ == '__main__':
    main()

