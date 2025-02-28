from .processor import KafkaInterface, ProcessingMetrics

def run_performance_test(
    processor: KafkaInterface,
    num_batches: int = 10,
    batch_size: int = 1000,
    timeout_ms: int = 1000
) -> ProcessingMetrics:
    """Run performance test and aggregate metrics"""
    total_metrics = ProcessingMetrics(
        messages_processed=0,
        total_time=0,
        avg_processing_time=0,
        throughput=0,
        batch_sizes=[],
        batch_processing_times=[]
    )
    
    try:
        for _ in range(num_batches):
            metrics = processor.process_batch(batch_size, timeout_ms)
            
            total_metrics.messages_processed += metrics.messages_processed
            total_metrics.total_time += metrics.total_time
            total_metrics.batch_sizes.extend(metrics.batch_sizes)
            total_metrics.batch_processing_times.extend(metrics.batch_processing_times)
            
            if metrics.messages_processed == 0:
                break
    
    finally:
        processor.close()
    
    # Calculate aggregated metrics
    if total_metrics.messages_processed > 0:
        total_metrics.avg_processing_time = total_metrics.total_time / total_metrics.messages_processed
        total_metrics.throughput = total_metrics.messages_processed / total_metrics.total_time
    
    return total_metrics

if __name__ == "__main__":
    # Example usage
    processor = SimpleMovingAverageProcessor(
        window_size=10,
        input_topic='market_data',
        output_topic='processed_data'
    )
    
    metrics = run_performance_test(
        processor,
        num_batches=10,
        batch_size=1000
    )
    
    print(f"Processed {metrics.messages_processed} messages")
    print(f"Average processing time: {metrics.avg_processing_time:.6f} seconds")
    print(f"Throughput: {metrics.throughput:.2f} messages/second")
    print(f"Total time: {metrics.total_time:.2f} seconds")