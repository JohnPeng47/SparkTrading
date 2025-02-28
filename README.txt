A learning project for Kafka/Spark, that runs a momentum based trading strategy against a synthetic data generator

Core Components

data_gen/strategies/momentum.py - Generates synthetic market data with configurable momentum patterns
pattern_detect/strategies/momentum_spark.py - Spark-based momentum pattern detector
pattern_detect/strategies/momentum_norm.py - Python-based momentum pattern detector for comparison
run.py - Main script to run the backtesting system

Infrastructure

kafka_setup.py - Manages Kafka topics (creation/deletion)
kafka_monitor.py - Real-time monitoring of Kafka topics
verify_install.py - Verifies Kafka and Spark installation

Setup Instructions

Install dependencies:

# Install poetry if you haven't already
pip install poetry

# Install project dependencies
poetry install

Start Kafka:

# Start Kafka services
docker-compose up -d

Setup Kafka topics:

# Create required topics
python kafka_setup.py

Verify installation:

python verify_install.py

Run the system:

python run.py

Monitor events (in a separate terminal):

python kafka_monitor.py market_data  # View generated market data
python kafka_monitor.py momentum_events  # View detected patterns
