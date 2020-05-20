package com.github.nullptr7.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static java.time.Duration.ofMillis;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class ConsumerDemoAssignAndSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);

        final Properties prop = new Properties();

        final String bootstrapServer = "127.0.0.1:9092";
        final String first_topic = "first_topic";

        // Create consumer
        prop.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        // Assign and seek  are mostly used to reply data or mostly fetch a specific message
        final TopicPartition topicPartition = new TopicPartition(first_topic, 0);
        consumer.assign(singletonList(topicPartition));

        final long offsetToStartReadingFrom = 15L;
        consumer.seek(topicPartition, offsetToStartReadingFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;


        // poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(ofMillis(100));

            for (ConsumerRecord<String, String> r : records) {
                numberOfMessagesReadSoFar++;
                logger.info("key - {}", r.key());
                logger.info("value - {}", r.value());
                logger.info("partition - {}", r.partition());
                logger.info("offset - {}", r.offset());
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Existing the application");
    }
}
