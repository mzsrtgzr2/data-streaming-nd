"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


# Tracks existing topics across all Producer instances
broker_properties = {
    'bootstrap.servers': 'PLAINTEXT://0.0.0.0:9092',
    'schema.registry.url': 'http://0.0.0.0:7070'
}
admin = AdminClient({
    'bootstrap.servers': broker_properties['bootstrap.servers']})
    
class Producer:
    """Defines and provides common functionality amongst Producers"""

    
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
        
        # Configure the AvroProducer
        self.producer = AvroProducer(
            broker_properties,
            # schema_registry=avro.CachedSchemaRegistryClient('http://localhost:7070'),
            default_key_schema=key_schema,
            default_value_schema=value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        if self.topic_name in admin.list_topics().topics:
            logger.info('topic %s already exists', self.topic_name)
            return

        topic = NewTopic(self.topic_name,
                         num_partitions=self.num_partitions,
                         replication_factor=self.num_replicas)

        logger.info('creating topic %s', self.topic_name)
        futures = admin.create_topics([topic])
        logger.info('wait creating topic %s', self.topic_name)
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic {topic} created")
            except Exception as e:
                logger.fatal(f"Failed to create topic {topic}: {e}")
        logger.info('finished creating topic %s', self.topic_name)
        
    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        
        self.producer.flush()

        # logger.info('deleting topic %s', self.topic_name)
        # admin = AdminClient(self.broker_properties)
        # admin.delete_topics([self.topic_name])


    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
