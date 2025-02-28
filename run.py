import time
from typing import List, Dict
from dataclasses import dataclass
from datetime import datetime, timedelta
from kafka_setup import KafkaTopicManager

from pattern_detect.processor import KafkaInterface, ProcessingMetrics
from data_gen.generate import MarketDataGenerator

@dataclass
class BacktestConfig:
    duration_seconds: int
    symbols: Dict
    ticks_per_second: int
    warmup_seconds: int = 5  # Time to wait for processors to initialize

@dataclass
class BacktestResults:
    start_time: datetime
    end_time: datetime
    total_messages: int
    processor_metrics: Dict[str, List[ProcessingMetrics]]
    
    def print_summary(self):
        print("\n=== Backtest Summary ===")
        print(f"Duration: {(self.end_time - self.start_time).total_seconds():.2f} seconds")
        print(f"Total messages generated: {self.total_messages}")
        print("\nProcessor Performance:")
        
        for processor_name, metrics_list in self.processor_metrics.items():
            if not metrics_list:
                print(f"\n{processor_name}: No metrics collected")
                continue
                
            # Calculate aggregates
            total_processed = sum(m.messages_processed for m in metrics_list)
            avg_latency = sum(m.avg_processing_time for m in metrics_list) / len(metrics_list)
            avg_throughput = sum(m.throughput for m in metrics_list) / len(metrics_list)
            
            print(f"\n{processor_name}:")
            print(f"  Total messages processed: {total_processed}")
            print(f"  Average latency: {avg_latency*1000:.2f} ms")
            print(f"  Average throughput: {avg_throughput:.2f} messages/second")
            print(f"  Message capture rate: {(total_processed/self.total_messages)*100:.1f}%")

class Backtester:
    def __init__(self, config: BacktestConfig):
        self.config = config
        
    def run(
        self,
        generator: MarketDataGenerator,
        processors: Dict[str, KafkaInterface]
    ) -> BacktestResults:
        print("Starting backtest...")
        print(f"Running for {self.config.duration_seconds} seconds")
        print(f"Processing {self.config.ticks_per_second} ticks/second")
        print(f"Using symbols: {self.config.symbols.keys()}")
        
        # Initialize results
        results = BacktestResults(
            start_time=datetime.now(),
            end_time=datetime.now(),
            total_messages=0,
            processor_metrics={}
        )
        
        # Initialize metrics collection for each processor
        for name in processors.keys():
            results.processor_metrics[name] = []
        
        try:
            # Start generator
            print("\nStarting data generation...")
            generator_thread = generator.run(self.config.ticks_per_second)
            
            # Warmup period
            print(f"Warming up for {self.config.warmup_seconds} seconds...")
            time.sleep(self.config.warmup_seconds)
            
            # Main backtest loop
            start_time = time.time()
            end_time = start_time + self.config.duration_seconds
                        
            print("\nRunning backtest...")
            while time.time() < end_time:
                # Collect metrics from each processor
                for name, processor in processors.items():
                    metrics = processor.process_batch(1000, timeout_ms=1000)
                    results.processor_metrics[name].append(metrics)
                
                time.sleep(0.1)  # Prevent tight loop
            
            results.end_time = datetime.now()
            results.total_messages = generator.get_total_messages()
            
        finally:
            # Cleanup
            print("\nShutting down...")
            generator.stop()
            for processor in processors.values():
                processor.close()
        
        return results


if __name__ == "__main__":
    from data_gen.strategies import momentum_pattern_generator
    from pattern_detect.strategies import SimpleMovingAverageProcessor, SparkMomentumProcessor
    from kafka_setup import KafkaBackTestSession

    topic_configs = {
        'market_data': {
            'num_partitions': 3,
            'replication_factor': 1
        },
        'momentum_events': {
            'num_partitions': 3,
            'replication_factor': 1
        }
    }
    with KafkaBackTestSession(topic_configs) as manager:    
        # Configure backtest
        config = BacktestConfig(
            duration_seconds=5,
            # Prices 2025-02-27
            symbols= {
                "APPL": 237.30,
                "GOOGL": 168.50,
                "MSFT": 392.53,
                "AMZN": 208.74
            },
            ticks_per_second=1
        )

        generator = MarketDataGenerator(config.symbols, tick_generator=momentum_pattern_generator)
        processors = {
            # "SparkMomentum": SparkMomentumProcessor(
            #     window_seconds=30,
            #     momentum_threshold=0.02,
            #     input_topic='market_data',
            #     output_topic='momentum_events'
            # ),
            "SimpleMomentum": SimpleMovingAverageProcessor(
                window_size=30,
                momentum_threshold=0.02,
                input_topic='market_data',
                output_topic='momentum_events'
            )
        }
        
        # Run backtest
        backtester = Backtester(config)
        results = backtester.run(generator, processors)
        
        # Print results
        results.print_summary()