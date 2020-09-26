# Kafka Twitter Consumer

- This is an example of using a Kafka Consumer to poll data from Kafka and ingest into an ElasticSearch cluster

#### Config settings:
The following environment variables should be set to connect to ElasticSearch:

    private static final String HOST_NAME = System.getenv("ELASTICSEARCH_HOST_NAME");
    private static final String USERNAME = System.getenv("ELASTICSEARCH_USERNAME");
    private static final String PASSWORD = System.getenv("ELASTICSEARCH_PASSWORD");
