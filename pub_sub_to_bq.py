import json
import time
import random
from google.cloud import pubsub_v1

# Initialize pub/sub client
publisher= pubsub_v1.PublisherClient()

#  Project_id and topic dtails
project_id="aerobic-datum-424113-v0"
topic="topic_order"
topic_path= publisher.topic_path(project_id,topic)

# Callback function to handle publishing result

def callback(future):
    # get the message id after publishing message
    try:
        message=future.result()
        print(f"Published message id : {message}")
    except Exception as err:
        print(f"Error in publishing message : {err}")

# Generate mock data
def generate_mock_data(order_id):
    items = ["Laptop", "Phone", "Book", "Tablet", "Monitor"]
    addresses = ["123 Main St, City A, Country", "456 Elm St, City B, Country", "789 Oak St, City C, Country"]
    statuses = ["Shipped", "Pending", "Delivered", "Cancelled"]

    return{"order_id":order_id,
           "customer_id": f"cust{random.randint(100,500)}",
           "item":random.choice(items),
           "customer_address":f"{random.choice(addresses)}",
           "price":round(random.uniform(100,1500),2),
           "order_status":f"{random.choice(statuses)}",
           "order_date":"2024-06-15"
           }

order_id=1
count=1

# publsihing message to pub/sub topic
while count<=200:
    try:    
        data=generate_mock_data(order_id)
        json_data=json.dumps(data).encode("utf8")
        publish_message=publisher.publish(topic_path,data=json_data)
        publish_message.add_done_callback(callback)
        print(publish_message.result())

    except Exception as e:
        print(f"Got exception while publishing message{e}")

    time.sleep(3)   # wait for 3 second to publish
    order_id+=1
    count+=1






