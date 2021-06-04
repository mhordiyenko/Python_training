# Pub/Sub: Publish retriever
# how to run example (cmd):
#                       python retrieve_message.py -pid some_project_id -sn some_subscription
import argparse
import google.api_core.exceptions
from google.cloud import pubsub_v1


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-pid', '--project_id', default='', help='Project ID. example: my-new-project')
    parser.add_argument('-sn', '--subscription_name', default='', help='Subscription name. example: my-sub')
    parameter = parser.parse_args()
    return parameter.project_id, parameter.subscription_name


def arguments_validation(project_id, subscription_name):
    if not project_id:
        raise Exception('\nPlease input -pid (project id) and -sn () parameters to retrieve messages.')

    if not subscription_name:
        raise Exception('\nPlease input -pid (project id) and -sn () parameters to retrieve messages.')


class PubSubMessageRecipient:

    def __init__(self, subscriber, project_id, subscription_name):
        self.subscriber = subscriber
        self.project_id = project_id
        self.subscription_name = subscription_name

    def is_subscription_exists(self, subscription_path):
        try:
            subscription = self.subscriber.get_subscription(subscription=subscription_path)
            return subscription is not None
        except google.api_core.exceptions.NotFound:
            print(f"The subscription '{self.subscription_name}' does not exist in the '{self.project_id}'"
                  f" project.")
            return False

    def retrieve_message(self):

        subscription_path = self.subscriber.subscription_path(project=self.project_id,
                                                              subscription=self.subscription_name)
        if self.is_subscription_exists(subscription_path):
            def callback(message):
                print(message.data)
                # message.ack()

            message_requester = self.subscriber.subscribe(subscription_path, callback=callback,
                                                          await_callbacks_on_shutdown=True)
            return message_requester.result()


if __name__ == '__main__':
    project_id, subscription_name = parse_arguments()
    arguments_validation(project_id, subscription_name)
    pubsub_subscriber = pubsub_v1.SubscriberClient()
    message_recipient = PubSubMessageRecipient(pubsub_subscriber, project_id, subscription_name)
    message_recipient.retrieve_message()
