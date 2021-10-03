"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest


        self.broker_properties = {
            'bootstrap.servers': 'PLAINTEXT://0.0.0.0:9092'
        }

        group_name = 'group1'

        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:7070"
            self.consumer = AvroConsumer(
                self.broker_properties,
                group_id=group_name,
                request_timeout_ms=self.consume_timeout*1000,
                auto_offset_reset='earliest' if self.offset_earliest else 'latest'
            )
        else:
            self.consumer = Consumer(
                self.broker_properties,
                group_id=group_name,
                request_timeout_ms=self.consume_timeout*1000,
                auto_offset_reset='earliest' if self.offset_earliest else 'latest'
            )

        self.consumer.subscribe(
            topics=[],
            pattern=self.topic_name_pattern,
            # listener=self.on_assign
        )

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        logger.info("on_assign is incomplete - skipping")
        for partition in partitions:
            pass
            #
            #
            # TODO
            #
            #

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
            msg = self.consumer.poll(self.consume_timeout)
        except SerializerError as e:
            logger.error("Message failed %s", e)
            return 0

        if msg is None:
            return 0
        if msg.error():
            logger.error("Consumer error: %s", msg.error())
            return 0

        self.message_handler(msg)   

    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()