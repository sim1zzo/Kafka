package com.sim1zzo.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServer = "127.0.0.1:9092";
//        create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = null;


        for(var i =0;i<10;i++){
//        create the producer
            producer = new KafkaProducer<String, String>(properties);

//        create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("first_topic", "Sending message " + i);
    //        send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
    //                execute everytime record is successfully sent or an exception is thrown
                    if(e == null){
    //                    record successfully sent
                        logger.info("Received new metadata: \n"+
                                "Topic: " + record.topic() + "\n" +
                                "Partitions: " + recordMetadata.partition() + "\n" +
                                "Offsets: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + new Date(recordMetadata.timestamp()).toString().substring(0,19));

                    }
                    else
                        logger.error("Error while producing: ", e);
                }
            });
        }

            producer.flush();
            producer.close();
    }
}
