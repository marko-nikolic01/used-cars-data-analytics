package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Date;
import java.util.Properties;

public class CalculateAveragePriceTrendsByModel {

    public static void main(String[] args) {
        // Kafka Streams configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "calculate-average-price-trends-per-model");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> input = builder.stream("transformed_data");

        // Cleaned stream with Listing objects
        KStream<String, Listing> listingStream = input.mapValues(value -> {
            ObjectMapper objectMapper = new ObjectMapper();

            try {
                JsonNode json = objectMapper.readTree(value);

                if (!json.hasNonNull("make") || 
                    !json.hasNonNull("model") ||
                    !json.hasNonNull("make_year") || 
                    !json.hasNonNull("price") ||
                    !json.hasNonNull("listing_date_time") ||
                    !json.hasNonNull("dealership_state") ||
                    !json.hasNonNull("longitude") || 
                    !json.hasNonNull("latitude") ||
                    !json.hasNonNull("vin")) {
                    return null;
                }

                double price = json.get("price").asDouble();
                if (price <= 0) {
                    return null;
                }

                return new Listing(
                    json.get("make").asText(),
                    json.get("model").asText(),
                    json.get("make_year").asInt(),
                    json.get("price").asDouble(),
                    json.get("listing_date_time").asText(),
                    json.get("dealership_state").asText(),
                    json.get("longitude").asDouble(),
                    json.get("latitude").asDouble(),
                    json.get("vin").asText()
                );

            } catch (Exception e) {
                return null;
            }
        }).filter((k, v) -> v != null)
          .selectKey((k, v) -> v.getMake() + "-" + v.getModel() + "-" + v.getYear());

        // 1-minute time window
        TimeWindows oneMinuteWindows = TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ZERO);

        // Aggregate rolling average price per carKey
        KTable<Windowed<String>, AveragePrice> avgTable = listingStream
            .groupByKey(Grouped.with(Serdes.String(), new ListingSerde()))
            .windowedBy(oneMinuteWindows)
            .aggregate(
                AveragePrice::new,
                (key, listing, agg) -> {
                    agg.add(listing.getPrice());
                    return agg;
                },
                Materialized.<String, AveragePrice, WindowStore<Bytes, byte[]>>as("one-minute-avg-store")
                    .withRetention(Duration.ofMinutes(1))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(new AveragePriceSerde())
            );

        // Write to MongoDB
        MongoDBWriter mongoDBWriter = new MongoDBWriter();
        listingStream.foreach((key, listing) -> {
            mongoDBWriter.writeListing(
                listing.getMake(),
                listing.getModel(),
                listing.getYear(),
                listing.getPrice(),
                listing.getListingDate(),
                listing.getDealershipState(),
                listing.getLongitude(),
                listing.getLatitude(),
                listing.getVin()
            );
        });

        avgTable.toStream().foreach((windowedKey, avg) -> {
            String[] parts = windowedKey.key().split("-");
            String make = parts[0];
            String model = parts[1];
            int year;
            try {
                year = Integer.parseInt(parts[2]);
            } catch (NumberFormatException e) {
                return;
            }

            Date windowEnd = new Date(windowedKey.window().end());
            mongoDBWriter.writeAverage(make, model, year, avg.getAverage(), windowEnd);
        });

        // Start Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}