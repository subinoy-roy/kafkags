package org.roy.kafkags;

import net.datafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.roy.kafkags.datasource.Data;
import org.roy.kafkags.schemas.EventType;
import org.roy.kafkags.schemas.User;
import org.roy.kafkags.schemas.UserEvent;

import java.time.Instant;
import java.util.Properties;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class SimpleKafkaProducer {
    // Logger for logging messages
    static Logger logger = LogManager.getLogManager().getLogger(SimpleKafkaProducer.class.getName());

    // Create a Faker instance to generate random data
    static Faker faker = new Faker();
    /**
     * This is a simple Kafka producer that sends messages to a specified topic.
     * It uses the KafkaProducer class from the Kafka client library.
     */
    public static void main(String[] args) {
        try (Producer<User, UserEvent> producer = getAvroProducer()) {
            String topic = AppConstants.TOPIC;
            // Produce 10 sample messages
            int cnt = 0;
            for (int i = 0; i < 1000; i++) {
                // Create a user with a random email ID
                String email = Data.UserEmails.get(faker.random().nextInt(0, Data.UserEmails.size() - 1));
                User user = User.newBuilder()
                        .setId(email)
                        .setName(Data.UserNames.get(email))
                        .build();

                // Create a random user event with the email
                UserEvent userEvent = UserEvent.newBuilder()
                        .setUserId(email)
                        .setEventType(EventType.valueOf(Data.Event.get(faker.random().nextInt(0, Data.Event.size() - 1))))
                        .setX(faker.random().nextInt(0, 1000))
                        .setY(faker.random().nextInt(0, 1000))
                        .setTimestamp(Instant.ofEpochSecond(System.currentTimeMillis()))
                        .build();

                if(userEvent.getEventType().equals(EventType.CLICK)){
                    cnt++;
                }
                // Create a ProducerRecord with the topic, key, and value
                ProducerRecord<User, UserEvent> record = new ProducerRecord<>(topic, user, userEvent);

                // Send the message asynchronously with a callback
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            // An error occurred
                            logger.log(java.util.logging.Level.SEVERE, "Error sending message", exception);
                        } else {
                            // Successfully sent
                            System.out.printf("Sent record to topic %s partition %d at offset %d%n",
                                    metadata.topic(), metadata.partition(), metadata.offset());
                        }
                    }
                });
                Thread.sleep((long) (Math.random() * 100)); // Sleep for a random time between 0 and 1000 milliseconds
            }
            System.out.println("Total Clicks: " + cnt);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        // Flush and close the producer to ensure all records are sent before shutdown.
    }

    // Create an Avro producer with the specified properties.
    /**
     * This method creates a Kafka producer configured to send Avro-serialized keys and values.
     *
     * @return A KafkaProducer instance
     */
    private static Producer<User, UserEvent> getAvroProducer() {
        Properties props = new Properties();
        // Bootstrap server is the address of the Kafka broker.
        props.put("bootstrap.servers", "172.24.244.170:9093");
        // Specify the serializer for keys
        props.put("key.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class.getName());
        // Specify the serializer for values (Avro)
        props.put("value.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class.getName());
        // Specify the schema registry URL
        props.put("schema.registry.url", "http://172.24.244.170:8081");

        // Create a KafkaProducer instance with the above properties
        return new KafkaProducer<>(props);
    }

}
