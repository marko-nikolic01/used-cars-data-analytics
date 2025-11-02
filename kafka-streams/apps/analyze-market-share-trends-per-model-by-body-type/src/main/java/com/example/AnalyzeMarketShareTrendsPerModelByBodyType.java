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

public class AnalyzeMarketShareTrendsPerModelByBodyType {

    public static void main(String[] args) {
        // Kafka Streams Configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "analyze-market-share-trends-per-model-by-body-type");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // Read from the transformed_data topic
        KStream<String, String> input = builder.stream("transformed_data");

        // Filter out invalid listings
        ObjectMapper mapper = new ObjectMapper();
        KStream<String, ModelListing> modelListingStream = input.mapValues(value -> {
            try {
                JsonNode json = mapper.readTree(value);

                if (!json.hasNonNull("make") ||
                    !json.hasNonNull("model") ||
                    !json.hasNonNull("make_year") ||
                    !json.hasNonNull("price") ||
                    !json.hasNonNull("listing_date_time")) {
                    return null;
                }

                int price = json.get("price").asInt();

                return new ModelListing(
                        json.get("make").asText(),
                        json.get("model").asText(),
                        json.get("make_year").asInt(),
                        price,
                        getPriceRange(price),
                        json.get("listing_date_time").asText()
                );

            } catch (Exception e) {
                return null;
            }
        })
        .filter((k, listing) -> listing != null)
        .selectKey((k, listing) ->
                listing.getPriceRange() + ";" +
                listing.getMake() + ";" +
                listing.getModel() + ";" +
                listing.getYear()
        );

        // 1-minute time window
        TimeWindows oneMinuteWindow = TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ZERO);

        // Count listings by model per price range category
        KTable<Windowed<String>, Long> modelCounts =
            modelListingStream
                .groupByKey(Grouped.with(Serdes.String(), new ModelListingSerde()))
                .windowedBy(oneMinuteWindow)
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("model-price-range-counts"));

        // Count listings by price range category
        ConcurrentHashMap<String, Long> priceTotals = new ConcurrentHashMap<>();
        KTable<Windowed<String>, Long> priceCounts =
            modelListingStream
                .groupBy((key, listing) -> listing.getPriceRange(),
                        Grouped.with(Serdes.String(), new ModelListingSerde()))
                .windowedBy(oneMinuteWindow)
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("price-range-counts"));

        priceCounts.toStream().foreach((windowedKey, totalCount) -> {
            String range = windowedKey.key();
            if (totalCount == null) {
                priceTotals.remove(range);
            } else {
                priceTotals.put(range, totalCount);
            }
        });

        // Calculate market share by model per price range category
        KStream<String, ModelMarketShare> marketShareStream = modelCounts.toStream()
            .flatMap((windowedKey, modelCount) -> {
                String key = windowedKey.key();
                String[] p = key.split("\\;");
                if (p.length != 4) return Collections.emptyList();

                String priceRange = p[0];
                String make = p[1];
                String model = p[2];
                int year = Integer.parseInt(p[3]);

                Long total = priceTotals.get(priceRange);
                if (total == null || total == 0) return Collections.emptyList();

                double share = (double) modelCount / total;

                ModelMarketShare result = new ModelMarketShare(
                        make, model, year, priceRange,
                        modelCount,
                        total,
                        share
                );

                return Collections.singletonList(
                        new KeyValue<>(key, result)
                );
            });

        // Save to database
        MongoDBWriter mongoDBWriter = new MongoDBWriter();
        marketShareStream.foreach((key, share) -> {
            mongoDBWriter.writeToMongo(
                    share.getMake(),
                    share.getModel(),
                    share.getYear(),
                    share.getPriceRange(),
                    share.getMarketShare(),
                    share.getModelCount(),
                    share.getPriceRangeTotal(),
                    new Date()
            );
        });

        // Start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    // Get price range
    private static String getPriceRange(int price) {
        if (price < 10000) return "0-10k";
        if (price < 20000) return "10-20k";
        if (price < 30000) return "20-30k";
        if (price < 40000) return "30-40k";
        if (price < 50000) return "40-50k";
        if (price < 60000) return "50-60k";
        if (price < 70000) return "60-70k";
        if (price < 80000) return "70-80k";
        if (price < 90000) return "80-90k";
        if (price < 100000) return "90-100k";
        return "100k+";
    }
}
