import json
import time
from typing import Callable, List, Dict
from dataclasses import dataclass
from kafka import KafkaProducer, KafkaConsumer
from abc import ABC, abstractmethod


@dataclass
class ProcessingMetrics:
    messages_processed: int
    total_time: float
    avg_processing_time: float
    throughput: float  # messages per second
    batch_sizes: List[int]
    batch_processing_times: List[float]

class KafkaInterface(ABC):
    def __init__(
        self,
        input_topic: str,
        output_topic: str,
        bootstrap_servers: List[str] = ['localhost:9092'],
        group_id: str = None
    ):
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id or f"processor_{int(time.time())}"
        
        # Initialize Kafka connections
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=False
        )
    
    @abstractmethod
    def process_message(self, message: Dict) -> Dict:
        """Process a single message"""
        pass

    def process_batch(
        self,
        batch_size: int,
        timeout_ms: int = 1000
    ) -> ProcessingMetrics:
        """Process a batch of messages and return metrics"""
        messages = []
        batch_start = time.time()
        batch_metrics = ProcessingMetrics(
            messages_processed=0,
            total_time=0,
            avg_processing_time=0,
            throughput=0,
            batch_sizes=[],
            batch_processing_times=[]
        )

        # Collect batch of messages
        messages = self.consumer.poll(timeout_ms=timeout_ms, max_records=batch_size)
        
        if not messages:
            return batch_metrics

        # Process batch
        start_time = time.time()
        
        for topic_partition, partition_messages in messages.items():
            for message in partition_messages:
                processed = self.process_message(message.value)
                if processed:
                    self.producer.send(self.output_topic, processed)
                batch_metrics.messages_processed += 1

        self.producer.flush()
        self.consumer.commit()
        
        # Calculate metrics
        end_time = time.time()
        processing_time = end_time - start_time
        
        batch_metrics.total_time = processing_time
        batch_metrics.avg_processing_time = processing_time / batch_metrics.messages_processed if batch_metrics.messages_processed > 0 else 0
        batch_metrics.throughput = batch_metrics.messages_processed / processing_time if processing_time > 0 else 0
        batch_metrics.batch_sizes.append(batch_metrics.messages_processed)
        batch_metrics.batch_processing_times.append(processing_time)
        
        return batch_metrics

    def close(self):
        """Clean up Kafka connections"""
        self.producer.close()
        self.consumer.close()
