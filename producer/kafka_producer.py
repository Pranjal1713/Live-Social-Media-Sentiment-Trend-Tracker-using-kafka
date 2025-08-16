

import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def serializer(message):
    return json.dumps(message).encode('utf-8')

def create_producer():
    """Create Kafka producer with retry logic"""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],  # Use kafka:29092 if running in Docker
                value_serializer=serializer,
                retries=5,
                retry_backoff_ms=1000,
                request_timeout_ms=30000,
                acks='all'  # Wait for all replicas to acknowledge
            )
            logger.info("Kafka producer created successfully")
            return producer
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed to create producer: {e}")
            time.sleep(5)
    raise Exception("Failed to create Kafka producer after multiple attempts")



def main_producer():
    producer = create_producer()
    
    # Load mock data
    try:
        with open('./data/mock_social_media.json', 'r') as f:
            mock_data = json.load(f)
        logger.info(f"Loaded {len(mock_data)} mock social media posts")
    except FileNotFoundError:
        logger.error("Mock data file not found. Creating sample data...")
        mock_data = [
            {
                "text": "I love this new product! #amazing #happy",
                "user": "user123",
                "platform": "twitter",
                "user_followers": 1500,
                "likes": 25,
                "retweets": 5,
                "location": {"city": "New York", "country": "USA"}
            },
            {
                "text": "This service is terrible #disappointed #angry",
                "user": "user456",
                "platform": "facebook",
                "user_followers": 800,
                "likes": 2,
                "retweets": 0,
                "location": {"city": "London", "country": "UK"}
            }
        ]

    message_count = 0
    try:
        while True:
            # Get a random message
            random_message = random.choice(mock_data).copy()
            
            # Add timestamp
            random_message['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            logger.info(f"Sending message {message_count + 1}: {random_message['text'][:50]}...")
            
            # Send to Kafka topic
            future = producer.send('social-media-posts', random_message)
            
            # Wait for the message to be sent
            try:
                record_metadata = future.get(timeout=10)
                logger.info(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
                message_count += 1
            except Exception as e:
                logger.error(f"Failed to send message: {e}")
            
            # Sleep between messages
            time_to_sleep = random.randint(2, 8)
            time.sleep(time_to_sleep)
            
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        producer.close()
        logger.info(f"Producer stopped. Sent {message_count} messages.")

if __name__ == '__main__':
    main_producer()

