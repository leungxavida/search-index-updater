import json
import logging

from kafka import KafkaConsumer
from kafka.errors import KafkaError


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseConnector(object):
    """
    Base Class that connects to Kafka and subscribes to a particular topic
    """
    def __init__(self):
        self.topic = ''

    def listen(self, handler):
        """
        Listens to the Kafka updates and takes in a handler to process the information
        :param handler: function to process the updates
        :return:
        """
        try:
            logger.info('Listening on topic: {}'.format(self.topic))
            consumer = KafkaConsumer(self.topic)

            for msg in consumer:
                object_dict = self._extract_updated_object(msg)
                if object_dict:
                    handler(object_dict)

        except Exception as ex:
            if isinstance(ex, KafkaError):
                logger.error('Error with Kafka: {}'.format(ex))

    def _extract_updated_object(self, msg):
        """
        Extracts core object data from Kafka message
        :param msg: message received from Kafka
        :return:
        """
        value = getattr(msg, 'value', None)
        if not value:
            return None
        str_value = str(msg.value, 'utf-8')
        payload = json.loads(str_value)['payload']
        object_dict = payload['after']
        return object_dict
