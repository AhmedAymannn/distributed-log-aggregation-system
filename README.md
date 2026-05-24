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


### Data Flow

1. **Producers** generate application logs using SLF4J/Logback
2. **logback-kafka-appender** serializes logs to JSON and publishes to Kafka topic `app-logs`
3. **Kafka** (KRaft mode) buffers and distributes messages to consumer groups
4. **Aggregator** consumes logs in batches using Spring Kafka
5. **MongoDB** persists structured logs with automatic indexing and TTL cleanup

---

## ??? Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Language** | Java | 21 |
| **Framework** | Spring Boot | 3.5.14 |
| **Message Broker** | Apache Kafka | 7.5.0 (KRaft mode) |
| **Database** | MongoDB | 8.0 |
| **Build Tool** | Maven | Latest |
| **Containerization** | Docker & Docker Compose | Latest |
| **Log Appender** | logback-kafka-appender | 0.1.0 |
| **Log Encoder** | logstash-logback-encoder | 8.0 |
| **Kafka UI** | provectuslabs/kafka-ui | Latest |

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

### 1. Clone the Repository

```bash
git clone https://github.com/AhmedAymannn/distributed-log-aggregation-system.git
cd distributed-log-aggregation-system
```

### 2. Configure Environment (Optional)

Edit `Infrastructure/.env` to customize ports and credentials:

```env
MONGO_ROOT_USER=admin
MONGO_ROOT_PASSWORD=super_secret_secure_password_2026
KAFKA_CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk

PORT_KAFKA=9092
PORT_KAFKA_UI=8081
PORT_PRODUCER_ONE=3001
PORT_PRODUCER_TWO=3002
PORT_AGGREGATOR=3003
```

### 3. Start All Services

```bash
cd Infrastructure
docker-compose up --build
```

### 4. Verify Services

- **Producer One**: http://localhost:3001
- **Producer Two**: http://localhost:3002
- **Aggregator**: http://localhost:3003
- **Kafka UI**: http://localhost:8081
- **Kafka Broker**: localhost:9092
- **MongoDB**: localhost:27017

### 5. Stop Services

```bash
docker-compose down
```

To remove volumes (including MongoDB data):

```bash
docker-compose down -v
```

---

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

## ?? API Endpoints

### Producer Endpoints

Both producers expose REST endpoints for generating test logs:

**Producer One (Port 3001)**
- `POST /api/logs/info` - Generate INFO log
- `POST /api/logs/error` - Generate ERROR log
- `POST /api/logs/debug` - Generate DEBUG log

**Producer Two (Port 3002)**
- `POST /api/logs/info` - Generate INFO log
- `POST /api/logs/error` - Generate ERROR log
- `POST /api/logs/debug` - Generate DEBUG log

### Aggregator Endpoints

**Aggregator (Port 3003)**
- `GET /api/logs` - Retrieve all logs from MongoDB
- `GET /api/logs/service/{serviceName}` - Filter logs by service
- `GET /api/logs/level/{logLevel}` - Filter logs by log level
- `GET /api/logs/trace/{traceId}` - Filter logs by trace ID

---

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

2. **Update configuration**:
   - Edit `aggregator-secondary/pom.xml` (artifactId, name)
   - Edit `aggregator-secondary/src/main/resources/application.yml` (port, consumer group)
   - Change consumer group ID to create a separate consumer group

3. **Add to docker-compose.yml**:
   ```yaml
   aggregator-secondary:
     build:
       context: ../aggregator-secondary
       dockerfile: Dockerfile
     container_name: aggregator-secondary
     depends_on:
       - kafka
     environment:
       - SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:29092
     ports:
       - "${PORT_AGGREGATOR_TWO}:3004"
   ```

### Adding Custom Log Processing

Modify `aggregator/src/main/java/com/myorg/Service/AggregatorService.java` to add custom processing logic:

```java
@Service
public class AggregatorService {
    
    @KafkaListener(topics = "app-logs", groupId = "aggregator-group")
    public void consumeLogs(List<String> messages) {
        // Add your custom processing logic here
        messages.forEach(message -> {
            // Parse, transform, enrich, or forward logs
        });
    }
}
```

---

## ?? Troubleshooting

### Kafka Connection Issues

**Problem**: Producers cannot connect to Kafka

**Solution**:
1. Verify Kafka is running: `docker ps | grep kafka`
2. Check port mapping: Ensure port 9092 is not in use
3. Review Kafka logs: `docker logs kafka`
4. Verify `SPRING_KAFKA_BOOTSTRAP_SERVERS` in docker-compose.yml

### MongoDB Connection Issues

**Problem**: Aggregator cannot connect to MongoDB

**Solution**:
1. Verify MongoDB is running: `docker ps | grep mongo`
2. Check credentials in `.env` and `application.yml`
3. Review MongoDB logs: `docker logs log_aggregator_db`
4. Ensure MongoDB URI format is correct

### Logs Not Appearing in Kafka UI

**Problem**: Messages published but not visible in Kafka UI

**Solution**:
1. Verify topic name matches (`app-logs`)
2. Check Kafka UI connection settings
3. Ensure producers are actually generating logs
4. Review producer logs: `docker logs producer-one`

### Consumer Lag

**Problem**: Aggregator falling behind producers

**Solution**:
1. Increase batch size in aggregator configuration
2. Add more aggregator instances (horizontal scaling)
3. Check MongoDB write performance
4. Review aggregator error logs

### Port Conflicts

**Problem**: Services fail to start due to port conflicts

**Solution**:
1. Modify ports in `Infrastructure/.env`
2. Ensure no other applications are using the configured ports
3. Restart services: `docker-compose down && docker-compose up`

---


## ?? License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## ?? Author

**AhmedAymannn**

- GitHub: [@AhmedAymannn](https://github.com/AhmedAymannn)
- Project: [distributed-log-aggregation-system](https://github.com/AhmedAymannn/distributed-log-aggregation-system)

---

## ?? Acknowledgments

- [Spring Boot](https://spring.io/projects/spring-boot) for the excellent framework
- [Apache Kafka](https://kafka.apache.org/) for the robust messaging platform
- [MongoDB](https://www.mongodb.com/) for the flexible document database
- [logback-kafka-appender](https://github.com/danielwegener/logback-kafka-appender) for seamless Kafka integration

---

## ?? Additional Resources

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spring Kafka Reference](https://docs.spring.io/spring-kafka/reference/)
- [MongoDB Documentation](https://docs.mongodb.com/)
- [Docker Compose Reference](https://docs.docker.com/compose/)
- [Technical Details](kafka-communication-flow.txt) - Deep dive into Kafka communication flow

---

**Built with ?? using Java, Spring Boot, Kafka, and MongoDB**
