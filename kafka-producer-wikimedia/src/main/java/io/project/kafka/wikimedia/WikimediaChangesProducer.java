package io.project.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {

        String bootstrapServer = "[::1]:9092";

        //producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //safe producer configs for kafka <= 2.8
        //properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        //properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        //properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));

        //set high throughput producer configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        //create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        String topic = "wikimedia_recentchange";

        BackgroundEventHandler eventHandler= new WikimediaEventHandler(kafkaProducer,topic);
        String url="https://stream.wikimedia.org/v2/stream/recentchange";
        URI uriUrl = URI.create(url);
        EventSource.Builder esBuilder=new EventSource.Builder(uriUrl);
        BackgroundEventSource.Builder eventSource = new BackgroundEventSource.Builder(eventHandler,esBuilder);
        BackgroundEventSource source = eventSource.build();

        //start the event handling i.e. the producer in another thread
        source.start();

        //produce for 10 minutes and block
        TimeUnit.MINUTES.sleep(10);

    }
}
