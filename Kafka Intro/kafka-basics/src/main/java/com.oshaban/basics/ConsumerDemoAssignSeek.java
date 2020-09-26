package com.oshaban.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) {

        // Create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Assign and seek are mostly used to replay data or fetch a specific message

        // Assign
        TopicPartition partitionToReadFrom = new TopicPartition(TOPIC, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // Seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numMessagesToRead = 5;
        int numMessagesReadSoFar = 0;

        // Poll for new data
        while (numMessagesReadSoFar < numMessagesToRead ) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));

            for (ConsumerRecord<String, String> record: records) {
                log.info("Key: {}, Value: {}", record.key(), record.value());
                log.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                numMessagesReadSoFar++;

                if (numMessagesReadSoFar >= numMessagesToRead) break;
            }
        }

    }
}
