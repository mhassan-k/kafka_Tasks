package com.iti;

import com.sun.javafx.binding.StringFormatter;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleValueProducer {

    public static void main (String[] args){

        Properties props = new Properties ();
        String BootStrapServers ="localhost:9092";
       // String grp_id = "gtest03";
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , BootStrapServers );
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()) ;


        KafkaProducer<String,String>  FirstProducer = new KafkaProducer<String,String> (props);
        String topic ="test05";
        String value ="Hye Kafka66";

        ProducerRecord<String, String> record= new ProducerRecord<String, String> (topic,value);
        FirstProducer.send(record ,new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null){
                    System.out.println("Printing record metadata .....");
                    System.out.println( "\n\t topic:: " + recordMetadata.topic() + "\n"
                            + "\t partition: " + recordMetadata.partition() + "\n"
                            + "\t offset: " + recordMetadata.offset()+ "\n");
                } else {
                    System.out.println(e.toString());
                }
            }
        });
        FirstProducer.flush();
        FirstProducer.close();

    }
}
