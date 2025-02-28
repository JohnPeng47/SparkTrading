A learning project for Kafka/Spark, that runs a momentum based trading strategy against a synthetic data generator

├── README.md
├── data_gen/                           # Data Generation Package
│   ├── __init__.py
│   ├── generate.py                     # Main data generation interface
│   └── strategies/                     # Data Generation Strategies
│       ├── __init__.py
│       ├── basic.py                    # Basic random walk generator
│       └── momentum.py                 # Momentum pattern generator
├── pattern_detect/                     # Pattern Detection Package
│   ├── __init__.py
│   ├── detect.py                       # Detection interface
│   ├── processor.py                    # Base processor class
│   └── strategies/                     # Detection Strategies
│       ├── __init__.py
│       ├── base.py                     # Base strategy interface
│       ├── momentum_norm.py            # Python-based momentum detector
│       └── momentum_spark.py           # Spark-based momentum detector
├── docker-compose.yml                  # Kafka infrastructure setup
├── kafka_monitor.py                    # Real-time Kafka topic monitor
├── kafka_setup.py                      # Kafka topic management
├── run.py                             # Main execution script
└── verify_install.py                  # Installation verification


Install dependencies:

# Install poetry if you haven't already
pip install poetry

# Install project dependencies
poetry install

Start Kafka:

# Start Kafka services
docker-compose up -d

# Run the backtest
python run.py

# Monitor Kafka messages in real-time (in a separate terminal):
python kafka_monitor.py market_data  # View generated market data
python kafka_monitor.py momentum_events  # View detected patterns
