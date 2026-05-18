🌐 Distributed Log Aggregation System

A scalable, containerized log aggregation framework demonstrating microservices (producers) sending log/event data directly to a central Kafka broker, with one or more aggregators consuming and processing those logs. The system is decoupled and easy to extend, deployed via Docker Compose.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [Directory Structure](#directory-structure)
- [Usage](#usage)
- [Extending the System](#extending-the-system)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

This project provides a practical demonstration of distributed log/event aggregation using:

- **Log Producers:** Microservices generating event logs.
- **Kafka Broker:** A central, scalable message queue that stores events.
- **Log Aggregators:** Applications that consume and process logs from Kafka (for storage, analytics, or forwarding).

There are **no intermediary “agents”** or collectors—log producers write directly to Kafka; aggregators independently pull from Kafka.

---

## Architecture

```
+----------------+         +-------------------+         +----------------+
| Log Producer 1 | ----+   |                   |   +---- | Log Aggregator |
+----------------+     |   |                   |   |     +----------------+
                       +-->|      Kafka        |<--+
+----------------+     |   |    (Broker)       |   |     +----------------+
| Log Producer 2 | ----+   |                   |   +---- | Log Aggregator |
+----------------+         +-------------------+         +----------------+
```
- **Producers**: Directly emit logs to Kafka (`localhost:9092`).
- **Kafka**: Receives, stores, and buffers event messages in topics (e.g., `app-logs`).
- **Aggregators**: Subscribe to Kafka topics; process and act on received logs.

There are **no dedicated agents, sidecars, or file-collecting daemons**.

---

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/)

### Clone and Start

```bash
git clone https://github.com/AhmedAymannn/distributed-log-aggregation-system.git
cd distributed-log-aggregation-system
docker-compose up --build
```

Services are accessible at:
- Producer One: [http://localhost:3001](http://localhost:3001)
- Producer Two: [http://localhost:3002](http://localhost:3002)

Kafka is exposed at `localhost:9092` for both producers and aggregators.

---

## Directory Structure

```
docker-compose.yml          # Orchestrates all services
log-producer-one/          # Code for Producer 1
log-producer-two/          # Code for Producer 2
aggregator/                # Log aggregation/consumer service
kafka-communication-flow.txt # Technical docs about data flow and port mapping
```

---

## Usage

- **Producers emit logs/events**: Each microservice generates log messages sent directly to Kafka.
- **Kafka stores events**: Logs are buffered in topics, decoupling producers from consumers.
- **Aggregators consume events**: Log aggregator applications (can be more than one) subscribe to Kafka and process or persist logs.

_Example:_  
A Java producer logs a message; its Kafka client submits the event over TCP to Kafka, which stores it. The aggregator uses a Kafka consumer to read, transform, or display the log.

---

## Extending the System

### Add a Producer

1. Copy an existing `log-producer-*` directory as a template.
2. Update your new producer as needed.
3. Register it in `docker-compose.yml`:

    ```yaml
    services:
      new-producer:
        build:
          context: ./new-producer
          dockerfile: Dockerfile
        ports:
          - "3003:3003"
    ```

### Add an Aggregator

1. Create a new service that connects to Kafka at `localhost:9092` and subscribes to a topic.
2. Register it in `docker-compose.yml`.
3. Aggregators can filter, store, or visualize logs as needed.

---

## Contributing

Contributions, issues, and pull requests are welcome!
- Fork the repo, make your changes, and submit via Pull Request.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

## Contact

**Author:** [AhmedAymannn](https://github.com/AhmedAymannn)  
Project link: [AhmedAymannn/distributed-log-aggregation-system](https://github.com/AhmedAymannn/distributed-log-aggregation-system)
