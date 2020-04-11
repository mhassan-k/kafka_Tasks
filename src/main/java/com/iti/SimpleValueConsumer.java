package com.iti;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
public class SimpleValueConsumer {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(SimpleValueConsumer.class.getName());
        String bootstrapServers = "localhost:9092";
       String grp_id = "gtest05";
        String topic = "test05";
        //Creating consumer properties
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, grp_id);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //creating consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        //Subscribing
        consumer.subscribe(Arrays.asList(topic));
        //polling
        while(true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record: records){
                logger.info("Key: "+ record.key() + ", Value:" +record.value());
                logger.info("Partition:" + record.partition()+",Offset:"+record.offset());
            }


        }

    }
}
