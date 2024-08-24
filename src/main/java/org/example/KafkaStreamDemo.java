package org.example;

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
public class KafkaStreamDemo
{
    public static void main( String[] args )
    {
        Properties config=new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        StreamsBuilder builder = new StreamsBuilder();
        //Stream from kafka <Key,Value>
        KStream<String,String>kStream= builder.stream("word-count-input");
        //MapValue lower case
       KTable<String,Long>ktable= kStream.mapValues(lines->lines.toLowerCase())
        //flatMapValues
                .flatMapValues(word-> Arrays.asList(word.split(" ")))
       //selectKey==>this will arrange key that copied as value
                .selectKey((key,value)->(value))
        //GroupByKey before aggregation
                .groupByKey()
        //count
                .count();
        //to data written to kafka topic
        ktable.toStream().to("word-count-output", Produced.with(Serdes.String(),Serdes.Long()));
        KafkaStreams stream=new KafkaStreams(builder.build(),config);
        stream.start();
        //print topology of processor(node) and stream(edge) in a graph/topology
        System.out.print(stream.toString());
        //gracefully shutdown/close stream
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));

    }
}
