package com.github.nullptr7.kafka.tutorial;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class ProducerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {

        System.out.println("Hello World");

        // Create producer properties
        Properties prop = new Properties();
        final String bootstrapServer = "127.0.0.1:9092";
        prop.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        for (int i = 0; i < 5; i++) {
            // Create record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello World " + i);
            // Send data
            producer.send(record, (recordMetadata, exception) -> {

                if (exception == null) {
                    logger.info("Received record metadata");
                    logger.info("Topic: {}", recordMetadata.topic());
                    logger.info("Partition: {}", recordMetadata.partition());
                    logger.info("Offset: {}", recordMetadata.offset());
                    logger.info("Timestamp: {}", recordMetadata.timestamp());
                } else {
                    logger.error(exception.getLocalizedMessage());
                }

            });
        }
        producer.flush();
        producer.close();
    }
}
