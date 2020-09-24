package com.oshaban.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final String CONSUMER_KEY = System.getenv("TWITTER_API_KEY");
    private static final String CONSUMER_SECRET = System.getenv("TWITTER_API_KEY_SECRET");
    private static final String TOKEN = System.getenv("TWITTER_ACCESS_TOKEN");
    private static final String TOKEN_SECRET = System.getenv("TWITTER_TOKEN_SECRET");

    private static final String TOPIC = "twitter_tweets";
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

    private Logger log = LoggerFactory.getLogger(TwitterProducer.class);

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(10);

        // Create a twitter client
        final Client client = createTwitterClient(msgQueue);

        // Create a Kafka Producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping application...");
            log.info("Stopping twitter client");
            client.stop();
            log.info("Stopping Kafka producer");
            producer.close(); // Close producer so all messages in memory are safely sent
            log.info("Done!");
        }));

        try {
            // Attempts to establish connection
            client.connect();

            while (!client.isDone()) {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);

                if (msg != null) {
                    log.info(msg);
                    // Create and send a record to Kafka
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, null, msg);
                    producer.send(record, (RecordMetadata recordMetaData, Exception e) -> {
                        if (e != null) {
                            log.error("An error occurred", e);
                        }
                    });
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }
        log.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Set up followings
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        return new KafkaProducer<>(properties);
    }

}
