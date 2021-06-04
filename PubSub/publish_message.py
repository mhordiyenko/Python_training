# Pub/Sub: Publish message
# how to run example (cmd):
#                       python publish_message.py -pid some_project_id -t some_topic
import argparse
import google.api_core.exceptions
from google.cloud import pubsub_v1


class PubSubMessageSender:

    def __init__(self, publisher, project_id, topic_name):
        self.publisher = publisher
        self.project_id = project_id
        self.topic_name = topic_name

    def is_topic_exists(self, topic_path):
        try:
            topic = self.publisher.get_topic(topic=topic_path)
            return topic is not None
        except google.api_core.exceptions.NotFound:
            print(f"The topic '{topic_name}' does not exist in the '{project_id}' project.")
            return False

    def send_message(self):

        topic_path = self.publisher.topic_path(project=self.project_id, topic=self.topic_name)
        if self.is_topic_exists(topic_path):
            print('Please type your message: ')
            message_publisher = self.publisher.publish(topic_path, str.encode(input()))
            message_id = message_publisher.result()
            print(f"The message is successfully sent to the '{self.topic_name}' topic. The message id is: {message_id}")
            return True


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-pid', '--project_id', default='', help='Project ID. example: my-new-project')
    parser.add_argument('-t', '--topic', default='', help='Topic name. example: My_Topic')
    parameter = parser.parse_args()
    return parameter.project_id, parameter.topic


def arguments_validation(project_id, topic_name):
    if not project_id:
        raise Exception('\nThe -pid (project_id) parameter missing. Please input it to send a message')
    if not topic_name:
        raise Exception('\nThe -t (topic_name) parameter missing. Please input it to send a message')


if __name__ == '__main__':
    project_id, topic_name = parse_arguments()
    arguments_validation(project_id, topic_name)
    pubsub_publisher = pubsub_v1.PublisherClient()
    new_message = PubSubMessageSender(pubsub_publisher, project_id, topic_name)
    new_message.send_message()
