# Kafka Avro Producer and Streams Application

This project demonstrates the use of Kafka with Avro serialization for producing and processing user events. It includes a Kafka producer for sending Avro-serialized messages and a Kafka Streams application for processing those messages.

## Features

- **Kafka Producer**: Sends `UserEvent` objects serialized with Avro.
- **Kafka Streams**: Processes `UserEvent` messages, filtering events based on their type (e.g., `CLICK` events).
- **Schema Registry**: Uses Confluent Schema Registry for managing Avro schemas.

## Prerequisites

- Java 17 or higher
- Apache Kafka
- Confluent Schema Registry
- Maven