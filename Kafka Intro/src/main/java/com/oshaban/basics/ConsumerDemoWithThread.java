package com.oshaban.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String GROUP_ID = "my-sixth-application";
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) throws InterruptedException {

        // Latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        log.info("Creating the consumer thread");
        // Create consumer runnable
        Runnable myConsumerRunnable = new ConsumerRunnable(BOOTSTRAP_SERVERS, GROUP_ID, TOPIC, latch);

        // Start the thread
        Thread thread = new Thread(myConsumerRunnable);
        thread.start();

        // Add a shutdown hook - this is ran when we send a shutdown signal to our application
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();

            // Wait for the consumer threads to finish processing
            // This give them the chance to commit their offsets before shutting down
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                log.info("Application has exited");
            }
        }));


        // Blocks main thread until the latch has counted down to 0
        latch.await();
    }

    public static class ConsumerRunnable implements Runnable {

        private final Logger log = LoggerFactory.getLogger(ConsumerRunnable.class);

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;

            // Create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // Create consumer
            this.consumer = new KafkaConsumer<>(properties);

            // Subscribe our consumer to our topic(s)
            this.consumer.subscribe(Collections.singleton(topic));

        }

        @Override
        public void run() {
            // Poll for new data

            try {
                while (true) {
                    ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100L));

                    for (ConsumerRecord<String, String> record : records) {
                        log.info("Key: {}, Value: {}", record.key(), record.value());
                        log.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                    }
                }
            } catch (WakeupException e) {
                log.info("Received shutdown signal");
            } finally {
                consumer.close();
                // Tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // wakeup() is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }

    }

}
