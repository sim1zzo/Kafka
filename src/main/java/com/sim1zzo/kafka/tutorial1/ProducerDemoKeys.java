package com.sim1zzo.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    private static int index = 1;
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServer = "127.0.0.1:9092";
//        create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(var i = 0; i < 10; i++ ){
            String topic = "first_topic";
            String value = "Sending message: " + i;
            String key = "id_" + i;

//        create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic,key, value);

            logger.info("Key:"+ key); //log the key
    //        send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
    //                execute everytime record is successfully sent or an exception is thrown
                    if(e == null){
    //                    record successfully sent
                        logger.info("Received new metadata: \n"+
                                "MESSAGE NUMBER: " + index++  + "\n"+
                                "Topic: " + record.topic() + "\n" +
                                "Partitions: " + recordMetadata.partition() + "\n" +
                                "Offsets: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + new Date(recordMetadata.timestamp()).toString().substring(0,19));

                    }
                    else
                        logger.error("Error while producing: ", e);
                }
            }).get(); // block the send to make syncronouns
        }
        producer.flush();
        producer.close();
    }
}
