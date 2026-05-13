# Distributed Log Aggregation System

A microservice-based distributed log aggregation system built with multiple producers. Easily scalable and suitable for high-throughput logging scenarios, this project demonstrates containerized log producers orchestrated using Docker Compose and event-driven message delivery.

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [Directory Structure](#directory-structure)
- [Usage](#usage)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [License](#license)

## Project Overview

The Distributed Log Aggregation System provides a scalable and modular framework for collecting event messages from multiple producers. Each producer emits event messages that are delivered to an intermediate layer, enabling reliable separation between log emission and aggregation. This approach is suitable for situations requiring decoupled, asynchronous, and robust event handling in distributed systems.

## Architecture

- **Containerized Producers:** Individual microservices generating event messages.
- **Message Delivery Layer:** Delivers messages from producers to downstream components for aggregation, storage, or analysis.
- **Decoupling:** Producers and consumers are separated, allowing scalable, reliable, and asynchronous log handling.


## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/)

### Clone the Repository

```bash
git clone https://github.com/AhmedAymannn/distributed-log-aggregation-system.git
cd distributed-log-aggregation-system
```

### Build and Run

Start all services as defined in the `docker-compose.yml`:

```bash
docker-compose up --build
```

- The services will be available at:
  - Producer One: [http://localhost:3001](http://localhost:3001)
  - Producer Two: [http://localhost:3002](http://localhost:3002)

## Directory Structure


- `docker-compose.yml`: Defines and orchestrates producer services.
- `log-producer-one/`, `log-producer-two/`: Individual producer microservices, each responsible for emitting event messages.

## Usage

- Start the environment with Docker Compose.
- Each producer operates independently and emits event messages to the delivery layer.
- Consumers or aggregators can subscribe to event messages for further processing or storage.
- Easily extend the system by adding more producers.

## Configuration

To add a new producer, duplicate a producer directory and update the `docker-compose.yml`:

```yaml
services:
  new-producer:
    build:
      context: ./new-producer
      dockerfile: Dockerfile
    ports:
      - "3003:3003"
```

## Contributing

Contributions and suggestions are welcome! Please create issues and pull requests to improve the system.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
