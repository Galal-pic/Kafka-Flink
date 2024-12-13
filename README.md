# Kafka-MySQL-ML Project

## Overview

This project demonstrates a real-time pipeline using Kafka for message streaming, MySQL for data storage, and a pre-trained ML model for predictions.

### Components

1. **Producer**: Reads data from a CSV file and streams it to a Kafka topic.
2. **Consumer**: Consumes messages, applies predictions, and inserts them into a MySQL database.
3. **Database**: MySQL stores the processed data.
4. **Machine Learning**: Pre-trained model predicts fraud likelihood .

---

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Services

- **Kafka**: Ensure Kafka is running locally.
- **MySQL**: Start MySQL server.

### 3. Database Setup

```bash
python db/create_table.py
```

### 4. Start Producer

```bash
python src/kafka_consumer.py
```

### 5. Start Consumer

```bash
python src/kafka_consumer.py
```

### File Structure

```
project/
├── config/
│   ├── db_config.py           # Database configuration
│   ├── kafka_config.py        # Kafka configuration
│   ├── model_config.py        # Model configuration
├── db/
│   ├── create_table.py        # Script to create the database table
├── src/
│   ├── data_generator.py      # Logic for generating fraudulent data
│   ├── kafka_consumer.py      # Kafka consumer logic
│   ├── kafka_producer.py      # Kafka producer logic
│   ├── message_processor.py   # Kafka message processing
│   ├── db_handler.py          # Database interaction logic
│   ├── model_predictor.py     # ML model prediction logic
├── models/
│   └── XGb_model.joblib       # Pre-trained machine learning model
├── data/
│   └── fraud_0.1origbase.csv  # Input data file for producer
├── requirements.txt           # Python dependencies
└── README.md                  # Documentation for the project
```
