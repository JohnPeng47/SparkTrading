from pyspark.sql import SparkSession
from kafka import KafkaProducer, KafkaConsumer
import json

def test_spark():
    # Test Spark
    spark = SparkSession.builder \
        .appName("TestInstallation") \
        .getOrCreate()
    
    print("Spark installation successful!")
    spark.stop()

def test_kafka():
    # Test Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    # Send test message
    producer.send('test_topic', {'test': 'message'})
    producer.flush()
    print("Kafka producer test successful!")
    
    # Test Kafka Consumer
    consumer = KafkaConsumer(
        'test_topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Try to get one message
    for message in consumer:
        print("Kafka consumer test successful!")
        break
    
    producer.close()
    consumer.close()

if __name__ == "__main__":
    test_spark()
    test_kafka()