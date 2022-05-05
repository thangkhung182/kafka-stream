package com.trungnguyen.kafkastream.ch3;

import com.trungnguyen.kafkastream.clients.producer.MockDataProducer;
import com.trungnguyen.kafkastream.model.Purchase;
import com.trungnguyen.kafkastream.model.PurchasePattern;
import com.trungnguyen.kafkastream.model.RewardAccumulator;
import com.trungnguyen.kafkastream.util.serializer.JsonDeserializer;
import com.trungnguyen.kafkastream.util.serializer.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.Properties;

public class ZMartKafkaStreamsApp {
    private static final Logger logger = LogManager.getLogger(ZMartKafkaStreamsApp.class);

    public static void main(String[] args) {
        var streamsConfig = new StreamsConfig(getProperties());
        var purchaseJsonSerializer = new JsonSerializer<Purchase>();
        var purchaseJsonDeserializer = new JsonDeserializer<>(Purchase.class);
        var purchaseSerde = Serdes.serdeFrom(
                purchaseJsonSerializer,
                purchaseJsonDeserializer
        );

        var purchasePatternSerde = Serdes.serdeFrom(
                new JsonSerializer<>(),
                new JsonDeserializer<>(PurchasePattern.class)
        );

        var rewardSerde = Serdes.serdeFrom(
                new JsonSerializer<>(),
                new JsonDeserializer<>(RewardAccumulator.class)
        );

        var stringSerde = Serdes.String();
        var streamsBuilder = new StreamsBuilder();

        // Builds source and first processor
        var purchaseKStream = streamsBuilder.stream(
                    "transactions",
                Consumed.with(stringSerde, purchaseSerde)
        ).mapValues(p -> Purchase.builder(p)
                .maskCreditCard()
                .build()
        );

        // Builds the purchase pattern processor
        var patternKStream = purchaseKStream
                .mapValues(p -> PurchasePattern.builder(p).build());

        patternKStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("patterns"));
        patternKStream.to("patterns", Produced.with(stringSerde, purchasePatternSerde));

        // builds the reward processor
        var rewardsKStream = purchaseKStream.mapValues(
                p -> RewardAccumulator.builder(p).build()
        );

        rewardsKStream.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("rewards"));
        rewardsKStream.to("rewards", Produced.with(stringSerde, rewardSerde));

        // builds the storage link, the topic used by the storage consumer
        purchaseKStream.to("purchases", Produced.with(stringSerde, purchaseSerde));

        // used only to produce data for this application, not typical usage
        MockDataProducer.producePurchaseData();

        var kafkaStreams = new KafkaStreams(streamsBuilder.build(),streamsConfig);
        logger.info("ZMart First Kafka Streams Application Started");
        kafkaStreams.start();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Shutting down the Kafka Streams Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }

    private static Properties getProperties() {
        var props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "FirstZmart-Kafka-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
