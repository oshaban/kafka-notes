package com.oshaban;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class ElasticSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    private static final String HOST_NAME = System.getenv("ELASTICSEARCH_HOST_NAME");
    private static final String USERNAME = System.getenv("ELASTICSEARCH_USERNAME");
    private static final String PASSWORD = System.getenv("ELASTICSEARCH_PASSWORD");

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createElasticSearchClient();

        // test inserting data
        String json = "{\"foo1\": \"client1\"}";
        IndexRequest indexRequest = new IndexRequest("twitter").source(json, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();

        log.info("ID is: {}", id);

        client.close();

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


}
