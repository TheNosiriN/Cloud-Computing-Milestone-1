from google.cloud import pubsub_v1
import csv
import json
import os
import glob

# Search for the service account key JSON file and set the credentials
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Define project and topic details
project_id = "astute-citadel-449203-s0" 
topic_name = "testTopic"

# Create a publisher client and topic path
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}...")

# Path to your CSV file
csv_file_path = "Labels.csv" 

# Read the CSV file and publish each row as a message
with open(csv_file_path, mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    
    for row in csv_reader:
        # Serialize each row (a dictionary) into JSON
        message_data = json.dumps(row).encode('utf-8')
        
        try:
            # Publish the message
            future = publisher.publish(topic_path, message_data)
            future.result()  # Ensure the message is successfully published
            print(f"Published message: {row}")
        except Exception as e:
            print(f"Failed to publish message: {e}")
