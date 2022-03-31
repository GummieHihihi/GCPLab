from google.cloud import pubsub_v1
publisher = pubsub_v1.PublisherClient()

input_topic = "projects/nttdata-c4e-bde/topics/uc1-input-topic-1"
publisher.create_topic(input_topic)