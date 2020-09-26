# Kafka Twitter Producer

- This is an example of using a Kafka Producer to read data from twitter and ingest into Kafka

#### Config settings:
The following environment variables should be set to connect to the Twitter API:


    private static final String CONSUMER_KEY = System.getenv("TWITTER_API_KEY");
    private static final String CONSUMER_SECRET = System.getenv("TWITTER_API_KEY_SECRET");
    private static final String TOKEN = System.getenv("TWITTER_ACCESS_TOKEN");
    private static final String TOKEN_SECRET = System.getenv("TWITTER_TOKEN_SECRET");


#### To create the Kafka Topic:
- This will publish messages to the `twitter_tweets` topic

`kafka-topics --bootstrap-server localhost:9092 --topic twitter_tweets --create  --partitions 6 --replication-factor 1`