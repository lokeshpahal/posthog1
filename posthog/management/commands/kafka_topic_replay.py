from ee.kafka_client.helper import get_kafka_consumer

from django.core.management.base import BaseCommand
from django.db import connection
import datetime
import boto3
import json

ONE_MB = 1000000

TMP_FILE_NAME = '/Users/yakko/Desktop/ph/posthog/posthog/management/commands/tmp/store.json'

def reset_tmp_file():
    with open(TMP_FILE_NAME, "w") as file:
        file.write("[")

class Command(BaseCommand):
    help = "Consume from a Kafka topic into S3"

    def add_arguments(self, parser):
        parser.add_argument("--topic", default=None)

    def handle(self, *args, **options):
        if not options["topic"]:
            print("The argument --topic is required")
            exit(1)

        s3 = boto3.resource("s3")

        consumer = get_kafka_consumer(options["topic"])

        current_size = 0
        reset_tmp_file()
        for message in consumer:
            event = message.value
            current_size += message.serialized_value_size

            is_last_message_in_batch = current_size > 500*ONE_MB
            # if it fails to compare, just get the message
            # try:
            #     if datetime.datetime.fromisoformat(event['now']) < datetime.datetime.fromisoformat('2021-09-22T00:00:00.000+00:00'):
            #         continue
            # except:
            #     pass

            with open(TMP_FILE_NAME, "a") as file:
                file.write(event)
                if (not is_last_message_in_batch):
                    file.write(',')
                else:
                    file.write(']')

            
            # s3.meta.client.upload_file('/tmp/hello.txt', 'mybucket', 'hello.txt')

            if (is_last_message_in_batch):
                reset_tmp_file()
                pass
                # upload to s3 here

        



# ConsumerRecord(
#     topic="events_plugin_ingestion",
#     partition=15,
#     offset=339439977,
#     timestamp=1632220912766,
#     timestamp_type=0,
#     key=None,
#     value={
#         "uuid": "017c07f2-9c7e-0000-a0c7-00cf455171bb",
#         "distinct_id": "4500",
#         "ip": "54.147.40.62",
#         "site_url": "https://app.posthog.com",
#         "data": '{"api_key": "1LdP2jk7mntBNogtwT9qcmGLvuYPE86zs0iBlDeMrmw", "distinct_id": "4500", "event": "impression", "messageId": "172af87f-d5b9-48b4-9273-1f0b88f83380", "properties": {"$anon_distinct_id": "31afab17-001f-4e85-8452-23f8078090b1", "$app_build": "1.0.0", "$app_name": "Supr Daily 2.0", "$app_namespace": "com.supr.suprdaily", "$app_version": "3.1.80", "$browser": "Chrome WebView", "$brows^CstOrders,showSuprPassCard,autoAddSuprAccessToCart,showRecentIssuesPage,skuAutoAdd,showMoreSimilarProductsBtn,selfServeConfig,showOldCategoryViewOnSearch,sentryFilters,showAllOffers,pastOrdersNewTag,showSuprPassSidebarCard,showFaqExitState,showSuprPassHomepageCard,showSuprPassSavingsBannerOnCart", "context": 5171, "context_1_name": "featureCardId", "context_1_value": 14118, "context_2_name": "featureCardName", "context_2_value": "featured-card-1", "currentScreen": "collection-page", "distinct_id": "4500", "ionicBuild": false, "ionicPatchVersion": 19, "isAnonymousUser": false, "isSampledEvent": false, "isSuprCreditsEnabled": true, "isVirtualDevice": false, "modalName": "-", "networkType": "4g", "objectName": "subcategory", "objectTag": "_", "objectValue": 35, "position": 6, "referralScreen": "home", "sessionId": "1632220849060--e3ba8a7ccdb2554b", "suprAccessMember": false, "suprAccessValidity": 0, "suprCreditsBalance": 0, "suprWalletBalance": 0}, "timestamp": "2021-09-21T10:41:50.391Z", "type": "capture"}',
#         "team_id": 572,
#         "now": "2021-09-21T10:41:52.762666+00:00",
#         "sent_at": "",
#     },
#     headers=[],
#     checksum=None,
#     serialized_key_size=-1,
#     serialized_value_size=2851,
#     serialized_header_size=-1,
# )
