# Pub/Sub: Create topic, subscription
# how to run example (cmd):
#                       python create_topic_and_subscription.py -pid some_project_id -t some_topic -sn some_subscription
#                       python create_topic_and_subscription.py -pid some_project_id -t some_topic
import argparse
from google.cloud import pubsub_v1
import google.api_core.exceptions


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-pid', '--project_id', default='', help='Project ID. example: my-new-project')
    parser.add_argument('-t', '--topic', default='', help='Topic name. example: My_Topic')
    parser.add_argument('-sn', '--subscription_name', default='', help='Subscription name. example: my-sub')
    return parser


class PubSubChecker:

    def __init__(self, topic_path, sub_path):
        self.topic_path = topic_path
        self.sub_path = sub_path

    def if_topic_exist(self):
        try:
            publisher.get_topic(topic=self.topic_path)
            return True
        except google.api_core.exceptions.NotFound:
            return False

    def if_subscription_exist(self):
        try:
            subscriber.get_subscription(subscription=self.sub_path)
            return True
        except google.api_core.exceptions.NotFound:
            return False


class PubSubTopicCreator:

    def __init__(self, topic_path):
        self.topic_path = topic_path

    def create_new_topic(self):
        if check.if_topic_exist():
            print(f"The topic '{self.topic_path}' already exists.")
        else:
            print('Create new topic -> in progress')
            result = publisher.create_topic(name=self.topic_path)
            print(f"The topic '{result.name}' is crated.")


class PubSubSubscriptionCreator:

    def __init__(self, sub_path, topic_path):
        self.sub_path = sub_path
        self.topic_path = topic_path

    def create_new_subscription(self):
        if check.if_topic_exist() & check.if_subscription_exist():
            print(f"The subscription '{self.sub_path}' already exists.")
        else:
            print('Create new subscription -> in progress')
            result = subscriber.create_subscription(name=self.sub_path, topic=self.topic_path)
            print(f"The subscription '{result.name}' is crated.")


def check_inserted_params_and_run(p):  # p requires param value
    if p.project_id == '' and p.topic == '' and p.subscription_name == '':
        print('\nPlease input -pid, -t, -sn parameters to create topic and subscription or -pid, -t to create just '
              'topic.')

    if p.project_id != '' and p.topic != '' and p.subscription_name == '':
        print('\n')

        topic = PubSubTopicCreator(topic_path)
        topic.create_new_topic()

    if param.project_id != '' and param.topic != '' and param.subscription_name != '':
        print('\n')

        topic = PubSubTopicCreator(topic_path)
        subscription = PubSubSubscriptionCreator(subscription_path, topic_path)

        topic.create_new_topic()
        subscription.create_new_subscription()


parser = create_parser()
param = parser.parse_args()
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(project=param.project_id, topic=param.topic)
subscription_path = subscriber.subscription_path(project=param.project_id, subscription=param.subscription_name)
check = PubSubChecker(topic_path, subscription_path)

check_inserted_params_and_run(param)
