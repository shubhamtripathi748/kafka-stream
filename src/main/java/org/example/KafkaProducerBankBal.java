package org.example;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class KafkaProducerBankBal {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties config=new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //acks so usually fin application we use this acks concept
        config.setProperty(ProducerConfig.ACKS_CONFIG,"all");//this will guarantee strongest ack from all the leader/sync server

        //now we would like to retry for 3 time in case any failure happen in network
        config.setProperty(ProducerConfig.RETRIES_CONFIG,"3");
        //NOW PRODUCER should be idempotent
        config.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        KafkaProducer<String,String>producer=new KafkaProducer<String,String>(config);
        int i=0;
        while(true){
            producer.send(sendMessage("shubham"));
            Thread.sleep(100);
            producer.send(sendMessage("nishant"));
            Thread.sleep(100);
            producer.send(sendMessage("abhilash"));
            if(i++>100){
                break;
            }
        }
        producer.close();

    }
    public static ProducerRecord<String,String> sendMessage(String name){

        String pattern = "yyyy-MM-dd'T'HH:mm:ss";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        String date = simpleDateFormat.format(new Date());
        PersonTransaction pt=new PersonTransaction(name, ThreadLocalRandom.current().nextInt(1,100),date,0);
        String value= new Gson().toJson(pt);
        ProducerRecord<String,String>producerRecord=new ProducerRecord<String,String>("bank-balance-kafka-topic",name,value);
        return producerRecord;
    }
}
