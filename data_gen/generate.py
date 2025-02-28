import json
import time
from datetime import datetime
from typing import Dict, List, Callable
from kafka import KafkaProducer

class MarketDataGenerator:
    def __init__(
        self, 
        prices: Dict[str, float],
        tick_generator: Callable[[str, Dict[str, float]], dict],
        kafka_broker: str = 'localhost:9092'
    ):
        self.tick_generator = tick_generator
        self.prices = prices
        self.running = False
        self.generator_thread = None
        self.total_messages = 0
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    
    def _generate_data(self, ticks_per_second: int, topic: str):
        """Internal method that runs in a separate thread to generate market data"""
        sleep_time = 1.0 / ticks_per_second
        
        try:
            while self.running:
                for symbol in self.prices.keys():
                    tick_data = self.tick_generator(symbol, self.prices)
                    # Update internal price state
                    if 'price' in tick_data:
                        self.prices[symbol] = tick_data['price']
                    self.producer.send(topic, tick_data)
                    self.total_messages += 1
                
                self.producer.flush()
                time.sleep(sleep_time)
                
        except Exception as e:
            print(f"\nError in market data generation: {e}")
            self.running = False
    
    def run(self, ticks_per_second: int = 10, topic: str = 'market_data'):
        """Run the market data generator in a separate thread"""
        import threading
        
        print(f"Starting market data generation at {ticks_per_second} ticks/second")
        print(f"Publishing data for symbols: {self.prices.keys()}")
        
        self.running = True
        self.generator_thread = threading.Thread(
            target=self._generate_data,
            args=(ticks_per_second, topic),
            daemon=True
        )
        self.generator_thread.start()
        
        return self.generator_thread

    def stop(self):
        """Stop the market data generation thread"""
        self.running = False
        if self.generator_thread and self.generator_thread.is_alive():
            self.generator_thread.join(timeout=2.0)
        self.producer.close()

    def get_total_messages(self):
        return self.total_messages