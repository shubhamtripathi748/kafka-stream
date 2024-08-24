package org.example;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jdk.nashorn.internal.runtime.regexp.joni.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamBankBalance
{
    public static void main( String[] args )
    {
        Properties config=new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"bank-balance-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //exactly once processing!!
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE_V2);
        StreamsBuilder builder = new StreamsBuilder();
       // builder.

        //Stream from kafka <Key,Value>
        KStream<String,String>kStream= builder.stream("bank-balance-kafka-topic");
        //MapValue lower case
       KTable<String,String>ktable= kStream.groupByKey().aggregate(()->"0",KafkaStreamBankBalance::aggregate);

        //kStream.groupByKey().reduce(Integer::sum);
        //to data written to kafka topic
        ktable.toStream().to("bank-balance-kafka-topic-output", Produced.with(Serdes.String(),Serdes.String()));
        KafkaStreams stream=new KafkaStreams(builder.build(),config);
        stream.cleanUp();
        stream.start();
        //print topology of processor(node) and stream(edge) in a graph/topology
        System.out.print(stream.toString());
        //gracefully shutdown/close stream
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));

    }
    private static String aggregate(String key,String value,String aggregate){
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        PersonTransaction pt= gson.fromJson(value, PersonTransaction.class);
        System.out.println("value=>"+value);
        System.out.println("pt=.getAmount=>"+pt.getAmount());
        System.out.println("aggregate==>"+aggregate);
        int sum=Integer.valueOf(aggregate)+pt.getAmount();
        return String.valueOf(sum);
    }
}
