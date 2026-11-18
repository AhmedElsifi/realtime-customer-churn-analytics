# Real-Time Customer Churn Analytics

A real-time Big Data pipeline for processing customer information and analyzing churn patterns using Apache Kafka, Apache NiFi, Apache Spark Structured Streaming, and MongoDB.

## Overview

Customer churn occurs when customers stop using a company's services or cancel their subscriptions.

This project aims to build a real-time data processing pipeline that collects customer information, transforms incoming records, performs streaming analytics, and stores the resulting information for further analysis.

The system analyzes customer churn across demographic, behavioral, billing, and customer-support dimensions to identify patterns associated with subscription cancellations.

## Architecture

The proposed data pipeline follows this workflow:

**Python Data Generator → Kafka → Apache NiFi → Kafka → Spark Structured Streaming → MongoDB**

1. **Data Generation:** Generate or collect customer records using Python.
2. **Raw Data Ingestion:** Publish customer records to a Kafka raw topic.
3. **ETL Processing:** Use Apache NiFi to validate, clean, and transform incoming records.
4. **Processed Data Streaming:** Publish transformed records to a separate Kafka topic.
5. **Stream Processing:** Use Spark Structured Streaming to consume and process incoming records.
6. **Analytics:** Calculate churn statistics and analyze customer behavior.
7. **Data Storage:** Store processed customer information and analytics results in MongoDB.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Customer data generation and Kafka producer |
| Apache Kafka | Real-time message streaming |
| Apache NiFi | Data ingestion and ETL |
| Apache Spark Structured Streaming | Real-time data processing and analytics |
| PySpark | Data transformation and analytical operations |
| MongoDB | Data persistence |

## Planned Analytics

The project aims to provide the following analytical insights:

- Overall customer churn rate.
- Active versus churned customer distribution.
- Churn patterns across age groups and locations.
- Relationship between monthly charges and churn.
- Relationship between service usage and churn.
- Relationship between customer-support interactions and churn.

## Repository Structure

```text
producer/       Customer data generation and Kafka producer
nifi/           NiFi ETL workflows
spark/          Streaming ingestion, processing, analytics, and database integration
database/       Database documentation and queries
data/           Sample and raw datasets
config/         Application configuration examples
docs/           Architecture and project documentation
tests/          Component-level tests
```

## Getting Started

### Prerequisites

The project requires the following software:

- Python
- Apache Kafka
- Apache NiFi
- Apache Spark with Structured Streaming
- MongoDB

Exact versions and installation instructions will be documented as the development environment is finalized.

### Configuration

1. Clone the repository.
2. Install the required Python dependencies.
3. Configure the environment variables using the provided example configuration.
4. Start and configure the required services.
5. Configure the Kafka topics and NiFi workflow.
6. Run the data producer and Spark streaming application.

Detailed setup and execution instructions will be added as the implementation progresses.

## Team

This project is developed by a team of six students as part of the Big Data Technologies course.

Each team member is responsible for implementing a specific component of the pipeline, while collaborating on the integration of the complete system.

## Project Status

**Status:** In Development

This project is being developed for academic purposes to demonstrate real-time Big Data ingestion, ETL, stream processing, and analytics.

---

**Built with Python, Apache Kafka, Apache NiFi, Apache Spark, and MongoDB.**