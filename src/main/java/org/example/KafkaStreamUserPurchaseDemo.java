package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamUserPurchaseDemo
{
    public static void main( String[] args )
    {
        Properties config=new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"user-purchase-application");
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
        KStream<String,String>kstream= builder.stream("user-purchases");
        GlobalKTable<String,String>globalKTable= builder.globalTable("user-table");

        KStream<String,String>purchaseByUser=kstream.join(globalKTable,(k,v)->k,(purchase,user)->"purchase==>"+purchase +":::userData"+ user);

        //first arg is simple which stream u would like to join
        //2nd arg will tell what should be the key to output
        //3rd represent what value u would like to return to result
        KStream<String,String>allpurchasesPlusUser=kstream.leftJoin(globalKTable,(k,v)->k,
//third arg is what we are about to return
                (purchase,user)->{
            if(user!=null) {
                return "purchase==>" + purchase + ":::userData" + user;
            }
                    return "purchase==>" + purchase + ":::userData is empty";
            });



        //to data written to kafka topic
        purchaseByUser.to("user-purchases-enriched-inner-join", Produced.with(Serdes.String(),Serdes.String()));
        allpurchasesPlusUser.to("user-purchases-enriched-left-join",Produced.with(Serdes.String(),Serdes.String()));
        KafkaStreams stream=new KafkaStreams(builder.build(),config);

        stream.cleanUp();//only for dev,uat not for prod
        stream.start();
        //print topology of processor(node) and stream(edge) in a graph/topology
        System.out.print(stream.toString());
        //gracefully shutdown/close stream
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));

    }

}
