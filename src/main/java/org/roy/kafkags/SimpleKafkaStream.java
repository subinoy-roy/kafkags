package org.roy.kafkags;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.roy.kafkags.schemas.EventType;
import org.roy.kafkags.schemas.User;
import org.roy.kafkags.schemas.UserEvent;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SimpleKafkaStream {
    public static void main(String[] args) {
        Properties streamConfig = new Properties();
        // Set the necessary properties for the Kafka producer and consumer
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-events-stream");
        streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.24.244.170:9093");
        streamConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
        streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamConfig.put("schema.registry.url", "http://172.24.244.170:8081");

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                "http://172.24.244.170:8081");

        try (Serde<User> keySpecificAvroSerde = new SpecificAvroSerde<>();
             Serde<UserEvent> valueSpecificAvroSerde = new SpecificAvroSerde<>()) {
            keySpecificAvroSerde.configure(serdeConfig, true);
            valueSpecificAvroSerde.configure(serdeConfig, false);

            StreamsBuilder builder = new StreamsBuilder();
            // Define the input topic
            String inputTopic = AppConstants.TOPIC;
            // Define the output topic
            String outputTopic = "user-events-output-clicks";
            // Create a stream from the input topic
            KStream<User, UserEvent> inputStream = builder.stream(inputTopic, Consumed.with(keySpecificAvroSerde, valueSpecificAvroSerde))
                    .filter((user, userEvent) -> userEvent.getEventType().equals(EventType.CLICK)); // Filter for CLICK events;
            inputStream.to(outputTopic, Produced.with(keySpecificAvroSerde, valueSpecificAvroSerde));

            try (KafkaStreams streams = new KafkaStreams(builder.build(), streamConfig)) {
                // Start the Kafka Streams application
                streams.start();
                // Add shutdown hook to gracefully close the application
                Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
                // Wait for the application to finish processing
                System.out.println("Press Ctrl+C to exit");
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
