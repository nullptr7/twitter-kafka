package com.github.nullptr7.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class ConsumerDemo {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        final Properties prop = new Properties();

        final String bootstrapServer = "127.0.0.1:9092";
        final String groupId = "my-fifth-application";
        final String first_topic = "first_topic";

        // Create consumer
        prop.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(GROUP_ID_CONFIG, groupId);
        prop.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        // subscribe consumer to our topics
        // Below says subscribing to one topic
//        consumer.subscribe(Collections.singleton(first_topic));
        consumer.subscribe(Arrays.asList(first_topic));

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(ofMillis(100));

            records.forEach(r -> {
                logger.info("key - {}", r.key());
                logger.info("value - {}", r.value());
                logger.info("partition - {}", r.partition());
                logger.info("offset - {}", r.offset());
            });
        }
    }
}
