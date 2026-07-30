# 👨‍🔧 Distributed Log Aggregation System

[![Java Version](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5.14-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Kafka](https://img.shields.io/badge/Kafka-7.5.0-black.svg)](https://kafka.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-8.0-green.svg)](https://www.mongodb.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://www.docker.com/)

A production-ready, scalable distributed log aggregation system demonstrating microservices architecture with direct Kafka integration. The system features multiple log producers streaming events to a central Kafka broker, with aggregators consuming, processing, and persisting logs to MongoDB for analytics and monitoring.

---

## 📚 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [API Endpoints](#api-endpoints)
- [Monitoring](#monitoring)
- [Development](#development)
- [Extending the System](#extending-the-system)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## ?? Overview

This project implements a modern distributed logging architecture without intermediary agents or collectors. Log producers write directly to Kafka using the logback-kafka-appender, while aggregators independently consume from Kafka topics and persist structured logs to MongoDB with automatic TTL indexing.

### Key Design Principles

- **Direct Integration**: Producers connect directly to Kafka�no sidecars, agents, or file collectors
- **Decoupled Architecture**: Producers and aggregators operate independently through Kafka topics
- **Scalability**: Horizontal scaling of both producers and aggregators
- **Persistence**: MongoDB storage with 7-day automatic log expiration
- **Observability**: Kafka UI for real-time topic monitoring and message inspection

---


## ? Features

- **Direct Kafka Integration**: No intermediate agents�producers write directly to Kafka
- **KRaft Mode**: Kafka runs without Zookeeper for simplified deployment
- **Batch Processing**: Aggregator consumes logs in batches for optimal throughput
- **Structured Logging**: JSON-formatted logs with consistent schema across services
- **Auto-Expiring Logs**: MongoDB TTL index automatically removes logs after 7 days
- **Distributed Tracing**: Trace ID support for request correlation
- **Service Identification**: Each log includes source service name
- **Kafka UI**: Web-based interface for monitoring topics and messages
- **Environment Configuration**: Centralized .env file for easy customization
- **Hot Reload**: Configuration changes via application.yml

---

## ?? Prerequisites

Before running this project, ensure you have installed:

- **Docker** >= 20.10 ([Download](https://www.docker.com/get-started))
- **Docker Compose** >= 2.0 ([Download](https://docs.docker.com/compose/install/))
- **Java 21** (for local development, optional for Docker deployment)
- **Maven 3.8+** (for local development, optional for Docker deployment)

---

## ?? Quick Start



## ?? Configuration

### Kafka Configuration

Kafka runs in **KRaft mode** (Zookeeper-less) with the following key settings:

- **Cluster ID**: Configured in `.env`
- **Listeners**: PLAINTEXT (internal), PLAINTEXT_HOST (external)
- **Log Retention**: 1GB per topic
- **Replication Factor**: 1 (single-broker setup)

### Producer Configuration

Each producer uses `lgogback-sprin.xml` for Kafka appender configuration:

```xml
<appender name="kafka" class="com.github.danielwegener.logback.kafka.KafkaAppender">
    <encoder class="net.logstash.logback.encoder.LogstashEncoder"/>
    <topic>app-logs</topic>
    <keyingStrategy class="com.github.danielwegener.logback.kafka.keying.NoKeyKeyingStrategy"/>
</appender>
```

### Aggregator Configuration

The aggregator uses batch consumption for optimal performance:

```yaml
spring:
  kafka:
    listener:
      type: batch
    consumer:
      fetch.min.bytes: 500
      auto-offset-reset: earliest
      group-id: aggregator-group
```


## ?? Monitoring

### Kafka UI

Access the Kafka UI at **http://localhost:8081** to:

- View all topics (`app-logs`)
- Monitor consumer groups
- Inspect messages in real-time
- View consumer lag and offsets
- Manage topic partitions

### MongoDB

Connect to MongoDB using:

```bash
# Using mongosh
mongosh -u admin -p super_secret_secure_password_2026 --authenticationDatabase admin

# Connection string
mongodb://admin:super_secret_secure_password_2026@localhost:27017/logs_db
```

### Log Files

Producer logs are also written to local files:
- `log-producer-one/producer_1.log`
- `log-producer-two/producer_2.log`

---

## ?? Development

### Local Development (Without Docker)

#### Prerequisites
- Java 21
- Maven 3.8+
- Kafka running locally (port 9092)
- MongoDB running locally (port 27017)

#### Build and Run

```bash
# Build aggregator
cd aggregator
mvn clean package
java -jar target/aggregator.jar

# Build producer one
cd ../log-producer-one
mvn clean package
java -jar target/log-producer-one.jar

# Build producer two
cd ../log-producer-two
mvn clean package
java -jar target/log-producer-two.jar
```


## ?? Extending the System

### Adding a New Producer

1. **Copy existing producer**:
   ```bash
   cp -r log-producer-one log-producer-three
   ```

2. **Update configuration**:
   - Edit `log-producer-three/pom.xml` (artifactId, name)
   - Edit `log-producer-three/src/main/resources/application.yml` (port, app name)
   - Edit `log-producer-three/src/main/java/com/myorg/ProducerThree.java`

3. **Add to docker-compose.yml**:
   ```yaml
   producer-three:
     build:
       context: ../log-producer-three
       dockerfile: Dockerfile
     container_name: producer-three
     ports:
       - "${PORT_PRODUCER_THREE}:3004"
     environment:
       - SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:29092
   ```

4. **Add to .env**:
   ```env
   PORT_PRODUCER_THREE=3004
   ```

### Adding a New Aggregator

1. **Copy existing aggregator**:
   ```bash
   cp -r aggregator aggregator-secondary
   ```



## ?? License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## ?? Author

**AhmedAymannn**

- GitHub: [@AhmedAymannn](https://github.com/AhmedAymannn)
- Project: [distributed-log-aggregation-system](https://github.com/AhmedAymannn/distributed-log-aggregation-system)

