# Pub/Sub: Create topic, subscription
# how to run example (cmd):
#                       python create_topic_and_subscription.py -pid some_project_id -t some_topic -sn some_subscription
#                       python create_topic_and_subscription.py -pid some_project_id -t some_topic
import argparse
import google.api_core.exceptions
from google.cloud import pubsub_v1


class PubSubTopicCreator:

    def __init__(self, publisher):
        self.publisher = publisher

    def create_new_topic(self, project_id, topic_name):
        topic_path = self.publisher.topic_path(project_id, topic_name)
        try:
            self.publisher.get_topic(topic=topic_path)
            return print(f"The topic '{topic_path}' already exists.")
        except google.api_core.exceptions.NotFound:
            print('Create new topic -> in progress')
            result = self.publisher.create_topic(name=topic_path)
            return print(f"The topic '{result.name}' is crated.")


class PubSubSubscriptionCreator:

    def __init__(self, publisher, subscriber):
        self.publisher = publisher
        self.subscriber = subscriber

    def create_new_subscription(self, project_id, topic_name, subscription_name):
        subscription_path = self.subscriber.subscription_path(project_id, subscription_name)
        try:
            self.subscriber.get_subscription(subscription=subscription_path)
            return print(f"The subscription '{subscription_path}' already exists.")
        except google.api_core.exceptions.NotFound:
            print('Create new subscription -> in progress')
            topic_path = self.publisher.topic_path(project=project_id, topic=topic_name)
            result = self.subscriber.create_subscription(name=subscription_path, topic=topic_path)
            return print(f"The subscription '{result.name}' is crated.")


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-pid', '--project_id', default='', help='Project ID. example: my-new-project')
    parser.add_argument('-t', '--topic', default='', help='Topic name. example: My_Topic')
    parser.add_argument('-sn', '--subscription_name', default='', help='Subscription name. example: my-sub')
    parameter = parser.parse_args()
    return parameter.project_id, parameter.topic, parameter.subscription_name


def arguments_validation(project_id, topic_name):

    if not project_id:
        print('\nProject Id parameter missing. Please input it with -pid')
        exit(1)
    if not topic_name:
        print('\nTopic name parameter missing. Please input it with -t.')
        exit(1)



if __name__ == '__main__':
    project_id, topic_name, subscription_name = parse_arguments()
    arguments_validation(project_id, topic_name)
    pubsub_publisher = pubsub_v1.PublisherClient()
    pubsub_subscriber = pubsub_v1.SubscriberClient()

    topic_creator = PubSubTopicCreator(pubsub_publisher)
    topic_creator.create_new_topic(project_id, topic_name)
    if subscription_name:
        subscription_creator = PubSubSubscriptionCreator(pubsub_publisher, pubsub_subscriber)
        subscription_creator.create_new_subscription(project_id, topic_name, subscription_name)


# projects/maximal-cabinet-310908/topics/moon
