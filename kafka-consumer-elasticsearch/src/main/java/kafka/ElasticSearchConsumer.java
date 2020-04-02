package kafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.elasticsearch.common.xcontent.XContentType.JSON;

public class ElasticSearchConsumer {

    final static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static RestHighLevelClient createClient() {

        String hostname = "hostname";
        String username = "username";
        String password = "password";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback((HttpAsyncClientBuilder httpAsyncClientBuilder) ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                );

        return new RestHighLevelClient(builder);

    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServer = "localhost:9092";
        String groupId = "kafka-demo-elasticsearch";

        Properties properties = new Properties();

        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(GROUP_ID_CONFIG, groupId);
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(MAX_POLL_RECORDS_CONFIG, "100");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(asList(topic));

        return consumer;
    }

    public static void main(String[] args) {
        //create a kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(ofMillis(100));

            Integer recordCount = records.count();
            logger.info("received " + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {

                //kafka generic id
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                try {
                    String id = extractIdFromTweet(record.value());
                    //where we insert data into elasticsearch
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).source(record.value(), JSON);
                    bulkRequest.add(indexRequest);

                } catch (NullPointerException e) {
                    logger.warn("bad data: " + record.value());
                }
            }
            if (recordCount > 0) {
                logger.info("commiting offsets");
                consumer.commitAsync();
                logger.info("offsets have been commited");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private static String extractIdFromTweet(String tweetJson) {
        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
