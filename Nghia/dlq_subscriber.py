import time

from google.cloud import pubsub_v1

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

# # pub/sub as output to dlq
dlq_topic = "projects/nttdata-c4e-bde/topics/uc1-dlq-topic-3"
dlq_sub = "projects/nttdata-c4e-bde/subscriptions/uc1-dlq-topic-sub-3"

# publisher.create_topic(dlq_topic)
# subscriber.create_subscription(name=dlq_sub, topic=dlq_topic)


# pub/ sub as input of dataflow
input_topic = "projects/nttdata-c4e-bde/topics/uc1-input-topic-1"
input_sub = "projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-1"

publisher.create_topic(input_topic)
subscriber.create_subscription(name=input_sub, topic=input_topic)



def callback(message):
    print(("Recieved Message: {}".format(message)))
    message.ack()


subscriber.subscribe(dlq_sub, callback=callback)

while True:
    time.sleep(1)
