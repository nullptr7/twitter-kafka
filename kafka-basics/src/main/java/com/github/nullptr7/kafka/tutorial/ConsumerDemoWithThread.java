package com.github.nullptr7.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.time.Duration.ofMillis;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class ConsumerDemoWithThread {


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

        final String bootstrapServer = "127.0.0.1:9092";
        final String groupId = "my-sixth-application";
        final String first_topic = "first_topic";

        // Latch for dealing multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //Create consumer runnable
        logger.info("Creating consumer thread");
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(bootstrapServer, groupId, first_topic, latch);

        //starting thread
        new Thread(myConsumerRunnable).start();

        // add a shutdown hook
        Runtime.getRuntime()
               .addShutdownHook(new Thread(() -> {
                   logger.info("Caught Shutdown hook");
                   myConsumerRunnable.shutdown();
                   try {
                       latch.await();
                   } catch (InterruptedException e) {
                       logger.error("Error in shutting down the application");
                   }
                   logger.info("Application exited");
               }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted!\n{}", e.getMessage());
        } finally {
            logger.info("Application is closing");
        }
    }

    public static class ConsumerRunnable implements Runnable {

        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServer, String groupId, String topicName, CountDownLatch latch) {
            Properties prop = new Properties();
            prop.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            prop.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(GROUP_ID_CONFIG, groupId);
            prop.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumer = new KafkaConsumer<>(prop);
            this.latch = latch;
            consumer.subscribe(singletonList(topicName));
        }

        @Override
        public void run() {
            // poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(ofMillis(150));

                    records.forEach(r -> {
                        logger.info("key - {}", r.key());
                        logger.info("value - {}", r.value());
                        logger.info("partition - {}", r.partition());
                        logger.info("offset - {}", r.offset());
                    });
                }
            } catch (WakeupException exception) {
                logger.error(exception.getMessage());
            } finally {
                consumer.close();
                // Tell are main code that we are done with countdown
                latch.countDown();
            }

        }

        public void shutdown() {
            //Special method to interrupt consumer.poll()
            //it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }


}



