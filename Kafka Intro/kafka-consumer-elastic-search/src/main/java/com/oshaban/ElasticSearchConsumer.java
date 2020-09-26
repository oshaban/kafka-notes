package com.oshaban;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


public class ElasticSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    private static final String HOST_NAME = System.getenv("ELASTICSEARCH_HOST_NAME");
    private static final String USERNAME = System.getenv("ELASTICSEARCH_USERNAME");
    private static final String PASSWORD = System.getenv("ELASTICSEARCH_PASSWORD");

    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String TOPIC = "twitter_tweets";
    private static final String GROUPID = "kafka-demo-elasticsearch";

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createElasticSearchClient();

        KafkaConsumer<String, String> consumer = createKafkaConsumer(Collections.singletonList(TOPIC));

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));

            int recordCount = records.count();

            log.info("Recieved {} records", records.count());

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records) {
                // Twitter feed specific id
                String tweetId = extractIdFromTweet(record.value());

                if (tweetId == null) {
                    log.warn("Skipping bad data: {}", record.value());
                } else {
                    // Insert data into ElasticSearch
                    IndexRequest indexRequest = new IndexRequest("twitter")
                            .source(record.value(), XContentType.JSON);
                    indexRequest.id(tweetId); // Sets a unique id for the ElasticSearch request, to ensure insertion is idempotent

                    // Add ElasticSearch record to a bulk request
                    bulkRequest.add(indexRequest);
                }
            }

            if (recordCount > 0) {
                // Bulk insert into ElasticSearch
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            }

            log.info("Committing the offsets...");
            consumer.commitSync();
            log.info("Offsets have been committed");
        }

    }

    private static RestHighLevelClient createElasticSearchClient() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(USERNAME, PASSWORD));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(HOST_NAME, 443, "https"))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(List<String> topics) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disable auto-commit of offsets

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topics);

        return consumer;
    }

    private static String extractIdFromTweet(String tweetJson) {
        try {
            return JsonParser.parseString(tweetJson).getAsJsonObject().get("id_str").getAsString();
        } catch (NullPointerException e) {
            return null;
        }
    }

}
