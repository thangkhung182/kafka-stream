package com.trungnguyen.kafkastream.ch3;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class YellingApp
{
    public static void main( String[] args ) throws InterruptedException {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling_app_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        var streamConfig = new StreamsConfig(props);
        Serde<String> stringSerde = Serdes.String();
        var builder = new StreamsBuilder();
        KStream<String, String> simpleFirstStream = builder.stream("src-topic",
                Consumed.with(stringSerde, stringSerde));

        KStream<String, String> upperCasedStream = simpleFirstStream.mapValues(str -> str.toUpperCase());
        upperCasedStream.to("out-topic", Produced.with(stringSerde, stringSerde));
        var kafkaStreams = new KafkaStreams(builder.build(), streamConfig);
        kafkaStreams.start();
        Thread.sleep(3500);
        System.out.println("Shutting down the yelling app now");
        kafkaStreams.close();
    }
}
