package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DetectCheapListings {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "detect-cheap-listings");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream("transformed_data");

        // Convert to Listing objects
        KStream<String, Listing> listingStream = input.mapValues(value -> {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
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
                if (price <= 0) return null;

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

        // Store every listing in ConcurrentHashMap
        ConcurrentHashMap<String, List<Listing>> listingsByKey = new ConcurrentHashMap<>();
        listingStream.foreach((key, listing) -> {
            listingsByKey.putIfAbsent(key, new ArrayList<>());
            listingsByKey.get(key).add(listing);
        });

        // 1-minute rolling average
        TimeWindows window = TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ZERO);

        KTable<Windowed<String>, AveragePrice> avgTable = listingStream
            .groupByKey(Grouped.with(Serdes.String(), new ListingSerde()))
            .windowedBy(window)
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

        // When new rolling average arrives, process all listings for that key
        KStream<String, Listing> enrichedListings = avgTable.toStream()
            .flatMap((windowedKey, avg) -> {

                String key = windowedKey.key();
                List<Listing> allListings = listingsByKey.remove(key);
                List<KeyValue<String, Listing>> out = new ArrayList<>();

                if (allListings == null || allListings.isEmpty()) return out;

                double averagePrice = avg.getAverage();

                for (Listing listing : allListings) {
                    double price = listing.getPrice();
                    double percentBelow = 0;

                    if (averagePrice > 0) {
                        percentBelow = ((averagePrice - price) / averagePrice) * 100.0;
                    }

                    Listing enriched = new Listing(
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
                    enriched.setPercentageBelowAveragePrice(percentBelow);

                    if(percentBelow >= 25.0) {
                        out.add(new KeyValue<>(key, enriched));
                    }
                }

                return out;
            });

        MongoDBWriter mongoDBWriter = new MongoDBWriter();
        enrichedListings.foreach((key, listing) -> {
            mongoDBWriter.writeToMongo(
                listing.getMake(),
                listing.getModel(),
                listing.getYear(),
                listing.getPrice(),
                listing.getListingDate(),
                listing.getDealershipState(),
                listing.getLongitude(),
                listing.getLatitude(),
                listing.getVin(),
                listing.getPercentageBelowAveragePrice()
            );
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
