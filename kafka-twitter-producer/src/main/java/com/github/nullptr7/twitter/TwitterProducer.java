package com.github.nullptr7.twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static com.twitter.hbc.core.Constants.STREAM_HOST;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class TwitterProducer {

    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private KafkaProducer<String, String> init() {
        // create kafka producer
        final Properties prop = new Properties();
        final String bootstrapServer = "127.0.0.1:9092";
        prop.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // properties for safe producer
        prop.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true");
        prop.setProperty(ACKS_CONFIG, "all");
        prop.setProperty(RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        prop.setProperty(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // Since we are using kafka 2.0 that is >= 1.1 so we can keep as 5 else use 1 otherwise

        prop.setProperty(COMPRESSION_TYPE_CONFIG, "snappy");
        prop.setProperty(BATCH_SIZE_CONFIG, Integer.toString(1024 * 32)); // 32KB batch size
        prop.setProperty(LINGER_MS_CONFIG, "20");


        return new KafkaProducer<>(prop);
    }

    private void produce(String data, KafkaProducer<String, String> producer) {

        ProducerRecord<String, String> record = new ProducerRecord<>("twitter_tweets", data);
        producer.send(record, (recordMetadata, exception) -> {
            if (exception == null) {
                logger.info("sending data offset {}", recordMetadata.offset());
            } else {
                logger.error("error while producing the record {}", exception.getMessage());
            }
        });
    }

    public void run() {

        logger.info("Setup");

        BlockingQueue<String> tweets = new LinkedBlockingQueue<>(30);

        Client client = createTwitterClient(tweets);
        client.connect();
        KafkaProducer<String, String> producer = init();

        Runtime.getRuntime()
               .addShutdownHook(new Thread(() -> {
                   logger.info("stopping application");
                   logger.info("Shutting down twitter client");
                   client.stop();
                   logger.info("Shutting down producer");
                   producer.close();
                   logger.info("done");
               }));

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = tweets.poll(5, TimeUnit.SECONDS);
                produce(msg, producer);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                logger.info("=".repeat(10));
                logger.info(msg);
                logger.info("=".repeat(10));
            }
        }

        logger.info("End of Application!");
    }

    public static void main(String[] args) {
        // Create a twitter client
        new TwitterProducer().run();


        // loop to send tweets
        // on a different thread, or multiple different threads....


    }

    public Client createTwitterClient(BlockingQueue<String> tweets) {

        final String CONSUMER_KEY = "";
        final String CONSUMER_SECRET = "";
        final String TOKEN = "";
        final String SECRET = "";

        Hosts hosts = new HttpHosts(STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        List<Long> followings = newArrayList(1234L, 566788L);
        List<String> terms = newArrayList("bantiktok");
        endpoint.followings(followings);
        endpoint.trackTerms(terms);

        Authentication authentication = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);

        ClientBuilder builder = new ClientBuilder().name("Twitter client")
                                                   .hosts(hosts)
                                                   .authentication(authentication)
                                                   .endpoint(endpoint)
                                                   .processor(new StringDelimitedProcessor(tweets));

        // Attempts to establish a connection.
        return builder.build();
    }
}
