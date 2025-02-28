from kafka import KafkaConsumer
import json
import argparse
from datetime import datetime


class KafkaMonitor:
    def __init__(self, topic: str, bootstrap_servers: str = 'localhost:9092'):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            auto_offset_reset='latest',  # Change this to 'earliest' to see all messages
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.topic = topic
        self.message_count = 0
        self.start_time = datetime.now()
        self.first_msg_time = None
        self.seen_timestamps = set()
        self.duplicate_count = 0

    def monitor(self, max_messages: int = None):
        print(f"\nMonitoring topic: {self.topic}")
        print("Press Ctrl+C to stop\n")
        
        try:
            for message in self.consumer:
                msg_timestamp = message.value.get('timestamp')
                
                if not self.first_msg_time:
                    self.first_msg_time = msg_timestamp
                
                # Check for duplicates
                if msg_timestamp in self.seen_timestamps:
                    self.duplicate_count += 1
                else:
                    self.seen_timestamps.add(msg_timestamp)
                
                self.message_count += 1
                elapsed = (datetime.now() - self.start_time).total_seconds()
                rate = self.message_count / elapsed if elapsed > 0 else 0
                
                print("\033[2J\033[H")  # Clear screen
                print(f"Topic: {self.topic}")
                print(f"Messages received: {self.message_count}")
                print(f"Unique messages: {len(self.seen_timestamps)}")
                print(f"Duplicate messages: {self.duplicate_count}")
                print(f"Messages/second: {rate:.2f}")
                print(f"\nFirst message time: {self.first_msg_time}")
                print(f"Latest message time: {msg_timestamp}")
                print("\nLast message:")
                print(json.dumps(message.value, indent=2))
                
                if max_messages and self.message_count >= max_messages:
                    break
                
        except KeyboardInterrupt:
            print("\nStopping monitor...")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Monitor Kafka topics')
    parser.add_argument('topic', help='Topic to monitor')
    parser.add_argument('--max', type=int, help='Maximum number of messages to show')
    args = parser.parse_args()

    monitor = KafkaMonitor(args.topic)
    monitor.monitor(args.max)
