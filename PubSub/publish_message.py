# Pub/Sub: Publish message
# how to run example (cmd):
#                       python create_topic_and_subscription.py -pid some_project_id -t some_topic
import argparse
import google.api_core.exceptions
from google.cloud import pubsub_v1


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-pid', '--project_id', default='', help='Project ID. example: my-new-project')
    parser.add_argument('-t', '--topic', default='', help='Topic name. example: My_Topic')
    parameter = parser.parse_args()
    if (not parameter.project_id) or (parameter.project_id == ''):
        print('\nPlease input -pid (project_id), -t (topic name) parameters to send a message')
        exit(1)
    if (not parameter.topic) or parameter.topic == '':
        print('\nPlease input -pid (project_id), -t (topic name) parameters to send a message')
        exit(1)
    return parameter.project_id, parameter.topic


class PubSubMessageSender:

    def __init__(self, project_id_obj, topic_name_obj):
        self.project_id = project_id_obj
        self.topic_name = topic_name_obj

    def send_message(self):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project=self.project_id, topic=self.topic_name)
        try:
            publisher.get_topic(topic=topic_path)
            print('Please type your message: ')
            message_publisher = publisher.publish(topic_path, str.encode(input()))
            message_id = message_publisher.result()
            return print(f"The message is successfully sent to the '{self.topic_name}' topic."
                         f" The message id is: {message_id}")
        except google.api_core.exceptions.NotFound:
            return print(f"The topic '{topic_name}' does not exist in the '{project_id}' project.")


if __name__ == '__main__':
    project_id, topic_name = parse_arguments()
    new_message = PubSubMessageSender(project_id_obj=project_id, topic_name_obj=topic_name)
    new_message.send_message()