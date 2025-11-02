package com.example;

import com.mongodb.client.*;
import org.bson.Document;

import java.util.Date;

public class MongoDBWriter {
    private static final String MONGO_URI = "mongodb://mongodb:27017";
    private static final String DATABASE_NAME = "used_cars";
    private static final String COLLECTION_NAME = "market_share_per_model_by_body_type";

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    public MongoDBWriter() {
        this.mongoClient = MongoClients.create(MONGO_URI);
        this.database = mongoClient.getDatabase(DATABASE_NAME);
        this.collection = database.getCollection(COLLECTION_NAME);
    }

    public void writeToMongo(String make, String model, int year, String priceRange, double marketShare, long modelCount, long priceRangeTotal, Date timestamp) {
        Document doc = new Document()
                .append("make", make)
                .append("model", model)
                .append("year", year)
                .append("price_range", priceRange)
                .append("market_share", marketShare)
                .append("model_count", modelCount)
                .append("price_range_total", priceRangeTotal)
                .append("timestamp", timestamp);

        collection.insertOne(doc);
    }
}
