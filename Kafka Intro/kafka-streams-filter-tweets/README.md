# Kafka Streams Example

- This is an example of using a Kafka Streams to filter twitter data
- Tweets from users with over 10,000 followers will be pushed to the `important_tweets` topic

#### Set up:
- Create the `important_tweets` topic before starting:

`kafka-topics --bootstrap-server localhost:9092 --topic important_tweets --create  --partitions 3 --replication-factor 1`

#### Consuming messages:
`kafka-console-consumer --bootstrap-server localhost:9092 --topic important_tweets --from-beginning`