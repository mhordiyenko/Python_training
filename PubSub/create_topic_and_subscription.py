# Pub/Sub: Create topic, subscription
# how to run example (cmd):
#                       python create_topic_and_subscription.py -pid some_project_id -t some_topic -sn some_subscription
#                       python create_topic_and_subscription.py -pid some_project_id -t some_topic
import argparse
import google.api_core.exceptions
from google.cloud import pubsub_v1


class PubSubCreator:

    def create(self):
        pass

class PubSubTopicCreator(PubSubCreator):

    def __init__(self, publisher_obj, project_id_obj, topic_name_obj):
        self.publisher = publisher_obj
        self.project_id = project_id_obj
        self.topic_name = topic_name_obj

    def create(self):
        topic_path = publisher.topic_path(project=self.project_id, topic=self.topic_name)
        try:
            self.publisher.get_topic(topic=topic_path)
            return print(f"The topic '{topic_path}' already exists.")
        except google.api_core.exceptions.NotFound:
            print('Create new topic -> in progress')
            result = self.publisher.create_topic(name=topic_path)
            return print(f"The topic '{result.name}' is crated.")


class PubSubSubscriptionCreator(PubSubCreator):

    def __init__(self, publisher_obj, subscriber_obj, project_id_obj, topic_name_obj, subscription_name_obj):
        self.publisher = publisher_obj
        self.subscriber = subscriber_obj
        self.project_id = project_id_obj
        self.topic_name = topic_name_obj
        self.subscription_name = subscription_name_obj

    def create(self):
        subscription_path = self.subscriber.subscription_path(project=self.project_id,
                                                              subscription=self.subscription_name)
        try:
            self.subscriber.get_subscription(subscription=subscription_path)
            return print(f"The subscription '{subscription_path}' already exists.")
        except google.api_core.exceptions.NotFound:
            print('Create new subscription -> in progress')
            topic_path = self.publisher.topic_path(project=self.project_id, topic=self.topic_name)
            result = self.subscriber.create_subscription(name=subscription_path, topic=topic_path)
            return print(f"The subscription '{result.name}' is crated.")


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-pid', '--project_id', default='', help='Project ID. example: my-new-project')
    parser.add_argument('-t', '--topic', default='', help='Topic name. example: My_Topic')
    parser.add_argument('-sn', '--subscription_name', default='', help='Subscription name. example: my-sub')
    parameter = parser.parse_args()
    if (not parameter.project_id) or (parameter.project_id == ''):
        print('\nPlease input -pid, -t, -sn parameters to create topic and subscription or -pid, -t to create just '
              'topic.')
        exit(1)
    if (not parameter.topic) or parameter.topic == '':
        print('\nPlease input -pid, -t, -sn parameters to create topic and subscription or -pid, -t to create just '
              'topic.')
        exit(1)
    if (not parameter.subscription_name) or parameter.subscription_name == '':
        subscription_name_inserted_obj = False
    else:
        subscription_name_inserted_obj = True
    return parameter.project_id, parameter.topic, parameter.subscription_name, subscription_name_inserted_obj


if __name__ == '__main__':
    project_id, topic_name, subscription_name, subscription_name_inserted = parse_arguments()
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    topic_creator = PubSubTopicCreator(publisher_obj=publisher, project_id_obj=project_id,
                                       topic_name_obj=topic_name)
    topic_creator.create()
    if subscription_name_inserted:
        subscription_creator = PubSubSubscriptionCreator(publisher_obj=publisher, subscriber_obj=subscriber,
                                                         project_id_obj=project_id, topic_name_obj=topic_name,
                                                         subscription_name_obj=subscription_name)
        subscription_creator.create()