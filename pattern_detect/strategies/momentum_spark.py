from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time
from typing import Dict
from pattern_detect.processor import KafkaInterface, ProcessingMetrics

class SparkMomentumProcessor(KafkaInterface):
    def __init__(
        self,
        window_seconds: int = 30,
        momentum_threshold: float = 0.02,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.window_seconds = window_seconds
        self.momentum_threshold = momentum_threshold
        
        # Initialize Spark with Kafka package
        self.spark = SparkSession.builder \
            .appName("SparkMomentumProcessor") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()
            
        self.schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", StringType(), True)
        ])
        
        self.query = None
        self._setup_streaming()
    
    def _setup_streaming(self):
        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", ",".join(self.bootstrap_servers)) \
            .option("subscribe", self.input_topic) \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Parse JSON
        parsed = df.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        ).select("data.*")

        # Convert timestamp and add window
        timestamped = parsed \
            .withColumn("event_timestamp", to_timestamp("timestamp")) \
            .withWatermark("event_timestamp", "10 seconds")
        
        # Create tumbling windows instead of range
        windowed = timestamped \
            .groupBy(
                "symbol",
                window("event_timestamp", f"{self.window_seconds} seconds")
            ) \
            .agg(
                first("price").alias("start_price"),
                last("price").alias("end_price"),
                collect_list("price").alias("prices")
            )
        
        # Calculate momentum
        momentum = windowed \
            .withColumn("price_change_pct", 
                (col("end_price") - col("start_price")) / col("start_price")
            ) \
            .filter(abs(col("price_change_pct")) > self.momentum_threshold) \
            .select(
                "symbol",
                "window.start",
                "window.end",
                "start_price",
                "end_price",
                "price_change_pct"
            )
        
        # Write to Kafka
        self.query = momentum \
            .selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", ",".join(self.bootstrap_servers)) \
            .option("topic", self.output_topic) \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .trigger(processingTime='1 second') \
            .start()

    def process_batch(
        self,
        batch_size: int,
        timeout_ms: int = 1000
    ) -> ProcessingMetrics:
        start_time = time.time()
        time.sleep(timeout_ms / 1000)
        
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

    def process_message(self, message: Dict) -> Dict:
        # Not used in Spark version - processing happens in streaming query
        pass