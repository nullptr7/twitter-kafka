package com.github.nullptr7.kafka.tutorial;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class ProducerDemoWithKeys {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // Create producer properties
        Properties prop = new Properties();
        final String bootstrapServer = "127.0.0.1:9092";
        prop.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        for (int i = 0; i < 10; i++) {
            // Create record
            String topicName = "first_topic";
            String dataValue = "Hello World with New Key " + i;
            String key = "id_" + i;

            logger.info("key - {}", key);

            // ID_0 - Partition 1
            // ID_1 - Partition 0
            // ID_2 - Partition 2
            // ID_3 - Partition 0
            // ID_4 - Partition 2

            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, dataValue);
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

            })
                    .get(); // Block the .send() to make it synchronous -- not recommended in prod
        }
        producer.flush();
        producer.close();
    }
}
