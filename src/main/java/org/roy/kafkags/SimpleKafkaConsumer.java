package org.roy.kafkags;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.roy.kafkags.schemas.User;
import org.roy.kafkags.schemas.UserEvent;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.LogManager;
import java.util.logging.Logger;

@Slf4j
public class SimpleKafkaConsumer {
    // Logger for logging messages
    static Logger logger = LogManager.getLogManager().getLogger(SimpleKafkaConsumer.class.getName());
    /**
     * This is a simple Kafka consumer that reads messages from a specified topic.
     * It uses the KafkaConsumer class from the Kafka client library.
     */
    public static void main(String[] args) {
        // Set properties for the Kafka Consumer
        Consumer<User, UserEvent> consumer = getAvroConsumer();

        // Subscribe to the topic of interest
        try (consumer) {
            consumer.subscribe(Collections.singletonList(AppConstants.TOPIC));
            // Continuously poll for new data from Kafka
            while (true) {
                // Poll with a timeout of 100 milliseconds
                ConsumerRecords<User, UserEvent> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<User, UserEvent> record : records) {
                    // Print details of each record received
                    System.out.printf("offset = %d, key = %s, value = %s%n",
                            record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e){
            logger.log(java.util.logging.Level.SEVERE, "Error in consumer", e);
        }
        // Always close the consumer gracefully to release resources
        consumer.close();
    }

    private static Consumer<User, UserEvent> getAvroConsumer() {
        Properties props = new Properties();
        // Specify the Kafka broker address
        props.put("bootstrap.servers", "172.24.244.170:9093");
        // Unique consumer group id for coordination among multiple consumers
        props.put("group.id", "test-group");
        // Enable auto commit of offsets
        props.put("enable.auto.commit", "true");
        // Frequency in milliseconds that the consumer offsets are auto-committed to Kafka
        props.put("auto.commit.interval.ms", "1000");
        // Specify the schema registry URL
        props.put("schema.registry.url", "http://172.24.244.170:8081");
        // Specify the deserializer class for the key
        props.put("key.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class.getName());
        // Specify the deserializer class for the value
        props.put("value.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class.getName());

        // Create a KafkaConsumer instance with the configuration properties
        return new KafkaConsumer<>(props);
    }
}

