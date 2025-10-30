package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class AnalyzeFuelTypeMarketShareTrendsByDealership {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "analyze-fuel-type-market-share-trends-by-dealership");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> input = builder.stream("transformed_data");

        ObjectMapper mapper = new ObjectMapper();

        // ✅ Extract and validate listings, key by dealership + fuel type
        KStream<String, FuelListing> listingStream = input.mapValues(value -> {
            try {
                JsonNode json = mapper.readTree(value);

                if (!json.hasNonNull("dealership") ||
                    !json.hasNonNull("fuel_type") ||
                    !json.hasNonNull("listing_date_time")) {
                    return null;
                }

                return new FuelListing(
                        json.get("dealership").asText(),
                        json.get("fuel_type").asText(),
                        json.get("listing_date_time").asText()
                );

            } catch (Exception e) {
                return null;
            }
        }).filter((k, listing) -> listing != null)
          .selectKey((k, listing) -> listing.getDealership() + "|" + listing.getFuelType());

        // ✅ Window size 1 hour
        TimeWindows oneMinuteWindow = TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ZERO);

        // ✅ Count number of listings for each dealership + fuel type
        KTable<Windowed<String>, Long> dealershipFuelCount =
                listingStream
                        .groupByKey(Grouped.with(Serdes.String(), new FuelListingSerde()))
                        .windowedBy(oneMinuteWindow)
                        .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("dealership-fuel-counts"));

        // ✅ Count total listings per fuel type (key becomes just fuel type)
        ConcurrentHashMap<String, Long> fuelTotals = new ConcurrentHashMap<>();
        KTable<Windowed<String>, Long> fuelTotalCount =
                listingStream
                        .groupBy((key, listing) -> listing.getFuelType(),
                                Grouped.with(Serdes.String(), new FuelListingSerde()))
                        .windowedBy(oneMinuteWindow)
                        .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("fuel-type-counts"));
        
        fuelTotalCount.toStream().foreach((windowedKey, totalCount) -> {
            String fuelType = windowedKey.key(); // fuel type
            if (totalCount == null) {
                fuelTotals.remove(fuelType); // handle deletion
            } else {
                fuelTotals.put(fuelType, totalCount);
            }
        });

        // ✅ Join to compute market share
        KStream<String, FuelMarketShare> marketShareStream = dealershipFuelCount.toStream()
            .flatMap((windowedKey, dealerCount) -> {
                    String key = windowedKey.key();
                    int sep = key.lastIndexOf('|');
                    if (sep == -1) return Collections.emptyList();

                    String dealership = key.substring(0, sep);
                    String fuelType = key.substring(sep + 1);

                    Long total = fuelTotals.get(fuelType);
                    if (total == null || total == 0) return Collections.emptyList();

                    double marketShare = dealerCount.doubleValue() / total;
                    FuelMarketShare share = new FuelMarketShare(dealerCount, total, marketShare);

                    return Collections.singletonList(new KeyValue<>(dealership + "|" + fuelType, share));
                });

        // ✅ Write into MongoDB for analytics
        MongoDBWriter mongoDBWriter = new MongoDBWriter();
        marketShareStream.foreach((key, share) -> {
            int sep = key.lastIndexOf('|');
            if (sep == -1) return;

            String dealership = key.substring(0, sep);
            String fuelType = key.substring(sep + 1);

            mongoDBWriter.writeToMongo(
                dealership, 
                fuelType, 
                share.getShare(), 
                share.getDealerCount(), 
                share.getFuelTotal(), 
                new Date());
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
