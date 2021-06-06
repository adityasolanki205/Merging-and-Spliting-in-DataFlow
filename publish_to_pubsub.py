#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from generating_data import generate_log
import logging
from google.cloud import pubsub_v1
import random
import time



PROJECT_ID="trusty-field-283517"
TOPIC = "german_credit_data"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC)


def publish(publisher, topic_path, message):
    data = message.encode('utf-8')
    return publisher.publish(topic_path, data = data)

def callback(message_future):
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic_path, message_future.exception()))
    else:
        print(message_future.result())

if __name__ == '__main__':

    while True:
        line = generate_log()
        print(line)
        message_future = publish(publisher, topic_path, line)
        message_future.add_done_callback(callback)

        sleep_time = random.choice(range(1, 3, 1))
        time.sleep(sleep_time)

