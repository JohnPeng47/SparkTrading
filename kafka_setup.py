from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from typing import List, Dict

class KafkaTopicManager:
    def __init__(self, bootstrap_servers: List[str] = ["localhost:9092"]):
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers
        )

    def create_topics(self, topic_configs: Dict[str, dict]) -> None:
        """
        Create topics with specified configurations
        Example config:
        {
            'market_data': {
                'num_partitions': 3,
                'replication_factor': 1
            }
        }
        """
        new_topics = []
        for topic_name, config in topic_configs.items():
            new_topics.append(NewTopic(
                name=topic_name,
                num_partitions=config.get("num_partitions", 1),
                replication_factor=config.get("replication_factor", 1)
            ))
        
        try:
            self.admin_client.create_topics(new_topics)
            print(f"Successfully created topics: {list(topic_configs.keys())}")
        except TopicAlreadyExistsError:
            print(f"Some topics already exist, skipping creation")
        except Exception as e:
            print(f"Error creating topics: {e}")

    def delete_topics(self, topics: List[str]) -> None:
        """Delete specified topics"""
        try:
            self.admin_client.delete_topics(topics)
            print(f"Successfully deleted topics: {topics}")
        except Exception as e:
            print(f"Error deleting topics: {e}")

    def close(self):
        self.admin_client.close()


class KafkaBackTestSession:
    def __init__(self, topic_configs: Dict[str, dict], bootstrap_servers: List[str] = ["localhost:9092"]):
        self.topic_configs = topic_configs
        self.bootstrap_servers = bootstrap_servers
        self.manager = None
        self.topics = list(topic_configs.keys())
        
    def __enter__(self):
        self.manager = KafkaTopicManager(bootstrap_servers=self.bootstrap_servers)
        self.manager.create_topics(self.topic_configs)
        # Give Kafka a moment to fully create topics
        import time
        time.sleep(2)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.manager:
            self.manager.delete_topics(self.topics)
            self.manager.close()

if __name__ == "__main__":
    # Example usage
    manager = KafkaTopicManager()
    try:
        # Create test topics
        manager.create_topics({
            'market_data': {
                'num_partitions': 3,
                'replication_factor': 1
            },
            'momentum_events': {
                'num_partitions': 3,
                'replication_factor': 1
            }
        })
    finally:
        manager.close()