package com.github.nullptr7.twitter;

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
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class TwitterMessageConsumer {
    private static final Logger logger = LoggerFactory.getLogger(TwitterMessageConsumer.class);

    public static void main(String[] args) {

        logger.info("Starting consumer...");

        final ElasticSearchClient elasticSearchClient = new ElasticSearchClient();
        final String TOPIC_NAME = "twitter_tweets";
        final String GROUP_ID = "twitter-application";
        final String BOOTSTRAP_SERVER = "localhost:9092";

        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating consumer thread");
        TwitterConsumerRunnable cons = new TwitterConsumerRunnable(BOOTSTRAP_SERVER, GROUP_ID, TOPIC_NAME, latch, elasticSearchClient);

        new Thread(cons).start();

        Runtime.getRuntime()
               .addShutdownHook(new Thread(() -> {
                   logger.info("Caught Shutdown hook");
                   cons.shutdown();
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

    private static final class TwitterConsumerRunnable implements Runnable {

        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;
        private final ElasticSearchClient ELK;

        public TwitterConsumerRunnable(String bootstrapServer, String groupId, String topicName, CountDownLatch latch, ElasticSearchClient ELK) {
            Properties prop = new Properties();
            prop.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            prop.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(GROUP_ID_CONFIG, groupId);
            //prop.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumer = new KafkaConsumer<>(prop);
            this.latch = latch;
            this.ELK = ELK;
            consumer.subscribe(singletonList(topicName));
        }

        @Override
        public void run() {
            try {
                int counterForRecords = 0;
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(ofMillis(1000));
                    logger.info("isEmpty {}", records.isEmpty());
                    logger.info("count {}", records.count());
                    logger.info("counterForRecords {}", counterForRecords);
                    if (records.isEmpty()) {
                        counterForRecords++;
                    } else {
                        counterForRecords = 0;
                    }
                    if (counterForRecords == 5) {
                        break;
                    }
                    records.forEach(r -> ELK.submitToElasticSearch(r.value()));
                }
            } catch (WakeupException exception) {
                logger.error(exception.getMessage());
            } finally {
                consumer.close();
                latch.countDown();
                ELK.closeClient();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }

}
