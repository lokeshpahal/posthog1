from django.core.management.base import BaseCommand
from django.db import connection

from ee.kafka_client.helper import get_kafka_consumer


class Command(BaseCommand):
    help = "Consume from a Kafka topic"

    def add_arguments(self, parser):
        parser.add_argument("--topic", default=None)

    def handle(self, *args, **options):
        if not options["topic"]:
            print("The argument --topic is required")
            exit(1)

        consumer = get_kafka_consumer(options["topic"])

        for message in consumer:
            val = message.value
            if "017c6add-8adb-0000-c368-01bc30f33abb" in val:
                print("partition: ", message.partition, message.value)
