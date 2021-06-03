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
    if (not parameter.project_id) or (parameter.project_id == ''):
        print('\nPlease input -pid (project id) and -sn () parameters to retrieve messages.')
        exit(1)
    if (not parameter.subscription_name) or parameter.subscription_name == '':
        print('\nPlease input -pid (project id) and -sn () parameters to retrieve messages.')
        exit(1)
    return parameter.project_id, parameter.subscription_name


class PubSubMessageRecipient:

    def __init__(self, project_id_obj, subscription_name_obj):
        self.project_id = project_id_obj
        self.subscription_name = subscription_name_obj

    def retrieve_message(self):
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project=self.project_id, subscription=self.subscription_name)
        try:
            subscriber.get_subscription(subscription=subscription_path)

            def callback(message):
                print(message.data)
                # message.ack()
            message_requester = subscriber.subscribe(subscription_path, callback=callback,
                                                     await_callbacks_on_shutdown=True)
            return message_requester.result()
        except google.api_core.exceptions.NotFound:
            return print(f"The subscription '{self.subscription_name}' does not exist in the '{self.project_id}'"
                         f" project.")


if __name__ == '__main__':
    project_id, subscription_name = parse_arguments()
    message_recipient = PubSubMessageRecipient(project_id_obj=project_id, subscription_name_obj=subscription_name)
    message_recipient.retrieve_message()