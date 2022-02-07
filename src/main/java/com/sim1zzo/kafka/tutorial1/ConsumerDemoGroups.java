package com.sim1zzo.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);

        var properties = new Properties();
        var bootstrapServer = "127.0.0.1:9092";
        var groupId = "my-fifth-application";
        var offset = "earliest";
        var topic = "first_topic";

        // create consumer config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);
        // AUTO_OFFSET_RESET_CONFIG - > [earlies, latest, none]  -> earliest if we want to read from the beginning, latest only the new messages onwards, none throw an error.


        // create a consumer
        var consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to topic(s)
//        consumer.subscribe(Collections.singleton(topic)); // in this way we subscribe to one topic
        // to subscribe to multiple topic we can use
         consumer.subscribe(Arrays.asList(topic));

        //poll for new data

        while(true){
            var records = consumer.poll(Duration.ofMillis(100));
            for(var record : records){
                logger.info("Key: " + record.key(), "Value: " + record.value());
                logger.info("Partition: " + record.partition(), "Offset: " + record.offset());
            }
        }
    }
}
