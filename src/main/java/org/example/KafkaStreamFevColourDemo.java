package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamFevColourDemo
{
    public static void main( String[] args )
    {
        Properties config=new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"favourite-colour-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //DISABLE THE CACHE //only for dev,uat not for prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");
        StreamsBuilder builder = new StreamsBuilder();
       // builder.
        //we are care about the latest record not the old record
        //Stream from kafka <Key,Value>
        KStream<String,String>kstream= builder.stream("favourite-colour-input");
        //MapValue lower case
        //userid,colour

        //keep only of colour "blue","green","red"
      KStream<String,String>kStreamTemp=
              //this is shubham,blue
              kstream.filter((k,v)->v.contains(",")).

                      mapValues(lines->lines.toLowerCase())
               //mark key as name
                      .selectKey((k,v)->v.split(",")[0])
                //mark colour as value
                      .mapValues(val-> val.split(",")[1])
                      //now filter out other colours-->data
                      .filter((user,colour)->Arrays.asList("blue","green","red").contains(colour))
              ;


        //favourite-colour-key is compacted topic
        kStreamTemp.to("favourite-colour-key-compact",Produced.with(Serdes.String(),Serdes.String()));


        KTable<String,String>userAndColourTable= builder.table("favourite-colour-key-compact");

         KTable<String,Long>favouriteColours=
                 userAndColourTable.groupBy((user,colour)->new KeyValue<>(colour,colour))
                         //.reduce((newStr,oldStr)->newStr.concat(oldStr),(newStr,oldStr)->newStr.concat(oldStr))
                         //.aggregate(()->0L /*init value*/
                         // ,(aggKey,newValue,aggValue)->aggValue+newValue.length() /*adder*/
                          //       ,(aggKey,newValue,aggValue)->aggValue-newValue.length());/*subtractor*/
                         .count();

        //to data written to kafka topic
        favouriteColours.toStream().to("favourite-colour-output-compact", Produced.with(Serdes.String(),Serdes.Long()));
        KafkaStreams stream=new KafkaStreams(builder.build(),config);

        stream.cleanUp();//only for dev,uat not for prod
        stream.start();
        //print topology of processor(node) and stream(edge) in a graph/topology
        System.out.print(stream.toString());
        //gracefully shutdown/close stream
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));

    }

}
