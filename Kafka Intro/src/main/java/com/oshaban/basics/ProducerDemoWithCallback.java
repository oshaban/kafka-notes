package com.oshaban.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

    public static void main(String[] args) {

        Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            // Create a Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world" + i);

            // Send data -- asynchronous
            // This send method takes a callback
            producer.send(record, (metadata, exception) -> {
                // Executes a record every time a record is successfully sent, or an exception is thrown
                if (exception == null) {
                    // The record was successfully sent
                    log.info("Recieved new metadata. \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp() + "\n");
                } else {
                    log.error("Error will producting", exception);
                }
            });

        }

        // Flush data
        producer.flush();

        // Flush and close producer
        producer.close();

    }
}