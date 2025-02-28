from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from typing import Dict

from kafka_interface import KafkaInterface, ProcessingMetrics

class SparkStreamingProcessor(KafkaInterface):
    def __init__(
        self,
        window_size: int = 10,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.window_size = window_size
        
        # Initialize Spark
        self.spark = SparkSession.builder \
            .appName("SparkStreamingProcessor") \
            .getOrCreate()
            
        # Define schema for market data
        self.schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("volume", IntegerType(), True)
        ])
        
        # Initialize streaming query
        self.query = None
        self._setup_streaming()
    
    def _setup_streaming(self):
        # Create streaming DataFrame from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", ",".join(self.bootstrap_servers)) \
            .option("subscribe", self.input_topic) \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Parse JSON and process
        parsed = df.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        ).select("data.*")
        
        # Calculate moving average
        windowed = parsed \
            .withWatermark("timestamp", "10 seconds") \
            .groupBy(
                "symbol",
                window("timestamp", f"{self.window_size} seconds", f"{self.window_size} seconds")
            ) \
            .agg(
                avg("price").alias("sma"),
                last("price").alias("price"),
                last("timestamp").alias("timestamp")
            )
        
        # Prepare output format
        output = windowed.select(
            col("symbol"),
            col("timestamp"),
            col("price"),
            col("sma")
        )
        
        # Write to Kafka
        self.query = output \
            .selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", ",".join(self.bootstrap_servers)) \
            .option("topic", self.output_topic) \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .trigger(processingTime='1 second') \
            .start()
    
    def process_message(self, message: Dict) -> Dict:
        # Not used in Spark version - processing happens in streaming query
        pass
    
    def process_batch(
        self,
        batch_size: int,
        timeout_ms: int = 1000
    ) -> ProcessingMetrics:
        start_time = time.time()
        
        # Wait for the specified timeout
        time.sleep(timeout_ms / 1000)
        
        # Get metrics from Spark streaming
        if self.query and self.query.lastProgress:
            progress = self.query.lastProgress
            
            return ProcessingMetrics(
                messages_processed=progress["numInputRows"],
                total_time=time.time() - start_time,
                avg_processing_time=progress.get("triggerProcessingTime", 0) / 1000,
                throughput=progress.get("processedRowsPerSecond", 0),
                batch_sizes=[progress["numInputRows"]],
                batch_processing_times=[progress.get("triggerProcessingTime", 0) / 1000]
            )
        
        return ProcessingMetrics(
            messages_processed=0,
            total_time=0,
            avg_processing_time=0,
            throughput=0,
            batch_sizes=[],
            batch_processing_times=[]
        )
    
    def close(self):
        if self.query:
            self.query.stop()
        if self.spark:
            self.spark.stop()

if __name__ == "__main__":
    # Example usage
    processor = SparkStreamingProcessor(
        window_size=10,
        input_topic='market_data',
        output_topic='processed_data'
    )
    
    try:
        while True:
            metrics = processor.process_batch(1000, 1000)
            print(f"Processed {metrics.messages_processed} messages")
            print(f"Throughput: {metrics.throughput:.2f} messages/second")
            print(f"Processing time: {metrics.avg_processing_time:.6f} seconds")
            time.sleep(1)
    except KeyboardInterrupt:
        processor.close()