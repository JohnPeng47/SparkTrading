import json
import time
from datetime import datetime
from typing import Dict, List, Callable
from kafka import KafkaProducer

class MarketDataGenerator:
    def __init__(
        self, 
        prices: List[Dict],
        tick_generator: Callable[[str, Dict[str, float]], dict],
        kafka_broker: str = 'localhost:9092'
    ):
        self.tick_generator = tick_generator
        self.prices = prices
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    
    def run(self, ticks_per_second: int = 10, topic: str = 'market_data'):
        """Run the market data generator"""
        sleep_time = 1.0 / ticks_per_second
        
        print(f"Starting market data generation at {ticks_per_second} ticks/second")
        print(f"Publishing data for symbols: {self.prices.keys()}")
        
        try:
            while True:
                for symbol in self.prices.keys():
                    tick_data = self.tick_generator(symbol, self.prices)
                    # Update internal price state
                    if 'price' in tick_data:
                        self.prices[symbol] = tick_data['price']
                    self.producer.send(topic, tick_data)
                
                self.producer.flush()
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            print("\nStopping market data generation...")
            self.producer.close()

if __name__ == "__main__":
    # Test with some sample symbols
    # Prices 2025-02-27
    symbols = {
        "APPL": 237.30,
        "GOOGL": 168.50,
        "MSFT": 392.53,
        "AMZN": 208.74
    }
    
    # Create generator with basic random walk
    generator = MarketDataGenerator(
        symbols=symbols,
        tick_generator=basic_random_walk_generator
    )
    
    # Or use momentum pattern generator
    # generator = MarketDataGenerator(
    #     symbols=symbols,
    #     tick_generator=lambda s, p: momentum_pattern_generator(s, p, trend_strength=0.003)
    # )
    
    generator.run(ticks_per_second=10)