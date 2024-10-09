package io.project.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200"; //using docker

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(){

        String bootstrapServers = "[::1]:9092";
        String groupId= "consumer-opensearch";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");

        //create consumer
        KafkaConsumer<String, String> kafkaConsumer=new KafkaConsumer<>(properties);

        return kafkaConsumer;
    }

    private static String extractId(String json){
        //gson library
        return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        //create OpenSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create Kafka Client
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        try(openSearchClient; kafkaConsumer) {

            boolean idxExist = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"),RequestOptions.DEFAULT);

            if(!idxExist) {
                //create index on OpenSearch if it doesnt exist
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("wikimedia index is created");
            }
            else {
                log.info("wikimedia index exists");
            }

            //subscribe the consumer
            kafkaConsumer.subscribe(Collections.singleton("wikimedia_recentchange"));

            //consume data
            while(true){
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received "+recordCount+" records");

                //sending data to elastic search
                for(ConsumerRecord<String, String> record: records){

                    // create id : strategy 1
                    //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    // extract id from json : strategy 2
                    String id = extractId(record.value());

                    IndexRequest indexRequest=new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(id);
                    IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                    log.info(response.getId());
                    log.info("Inserted 1 document into OpenSearch");
                }

                //commit offset after batch is consumed
                kafkaConsumer.commitSync();
                log.info("Offsets are commited");
            }
        }

        //close

    }
}
