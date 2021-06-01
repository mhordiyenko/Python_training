import argparse
from google.cloud import pubsub_v1


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-pid', '--project_id', default='', help='Project ID. example: my-new-project')
    parser.add_argument('-t', '--topic', default='', help='Topic name. example: My_Topic')
    return parser


class PubSubMessageSender:

    def __init__(self, topic_path, param):
        self.topic_path = topic_path
        self.param = param

    def send_message(self):
        print('Please type your message: ')
        message = publisher.publish(self.topic_path, str.encode(input()))
        message_id = message.result()
        print(f"The message is successfully sent to the '{self.param.topic}' topic. The message id is: {message_id}")


def check_inserted_params_and_run(param):  # param requires param value
    if (param.project_id == '' and param.topic == '') or (param.project_id != '' and param.topic == ''):
        print('\nPlease input -pid (project_id), -t (topic name) parameters to send a message')

    if param.project_id != '' and param.topic != '':
        print('\n')
        new_message = PubSubMessageSender(topic_path=topic_path, param=param)
        new_message.send_message()


publisher = pubsub_v1.PublisherClient()
parser = create_parser()
param = parser.parse_args()

topic_path = publisher.topic_path(project=param.project_id, topic=param.topic)
check_inserted_params_and_run(param=param)
