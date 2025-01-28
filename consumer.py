from google.cloud import pubsub_v1
import json
import os
import glob

# Search for the service account key JSON file and set the credentials
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Define project and subscription details
project_id = "astute-citadel-449203-s0"     # Replace with your GCP project ID
subscription_id = "testTopic-sub"   # Replace with your subscription name

# Create a subscriber client and subscription path
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
print(f"Listening for messages on {subscription_path}...\n")

# Callback function for processing received messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        # Deserialize the message into a dictionary
        message_data = json.loads(message.data.decode('utf-8'))
        print(f"Consumed record: {message_data}")
        
        # Acknowledge the message
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")

# Subscribe to the topic
with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
