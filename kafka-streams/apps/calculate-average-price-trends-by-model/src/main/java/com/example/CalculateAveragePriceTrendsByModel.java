package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class CalculateAveragePriceTrendsByModel {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "calculate-average-price-trends-pet-state");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> input = builder.stream("listings");

        // ───────────────────────────────────────────────
        // 2️⃣ Clean + parse into Listing objects
        // ───────────────────────────────────────────────
        KStream<String, Listing> listingStream = input.mapValues(value -> {
            try {
                JsonNode json = mapper.readTree(value);

                if (!json.hasNonNull("make") || !json.hasNonNull("model") ||
                    !json.hasNonNull("make_year") || !json.hasNonNull("price") ||
                    !json.hasNonNull("listing_date_time") ||
                    !json.hasNonNull("longitude") || !json.hasNonNull("latitude")) {
                    return null;
                }

                return new Listing(
                    json.get("make").asText(),
                    json.get("model").asText(),
                    json.get("make_year").asInt(),
                    json.get("price").asDouble(),
                    json.get("listing_date_time").asText(),
                    json.get("longitude").asDouble(),
                    json.get("latitude").asDouble()
                );

            } catch (Exception e) {
                return null;
            }
        }).filter((k, v) -> v != null)
          .selectKey((k, v) -> v.getMake() + "-" + v.getModel() + "-" + v.getYear());

        // ───────────────────────────────────────────────
        // 3️⃣ Write each listing to MongoDB (raw table)
        // ───────────────────────────────────────────────
        listingStream.foreach((key, listing) -> {
            mongoWriter.writeListing(
                listing.getMake(),
                listing.getModel(),
                listing.getYear(),
                listing.getPrice(),
                listing.getListingDate(),
                listing.getLongitude(),
                listing.getLatitude()
            );
        });

        // ───────────────────────────────────────────────
        // 4️⃣ 1-Minute Windowed Aggregation
        // ───────────────────────────────────────────────
        TimeWindows oneMinuteWindows = TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ZERO);

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

        // ───────────────────────────────────────────────
        // 5️⃣ Write per-minute averages to MongoDB
        // ───────────────────────────────────────────────
        avgTable.toStream().foreach((windowedKey, avg) -> {
            String[] parts = windowedKey.key().split("-");
            String make = parts[0];
            String model = parts[1];
            int year = Integer.parseInt(parts[2]);

            Date windowEnd = new Date(windowedKey.window().end());
            mongoWriter.writeAverage(make, model, year, avg.getAverage(), windowEnd);
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}