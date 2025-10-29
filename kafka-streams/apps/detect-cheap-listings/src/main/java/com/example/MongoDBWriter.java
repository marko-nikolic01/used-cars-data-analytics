package com.example;

import com.mongodb.client.*;
import org.bson.Document;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MongoDBWriter {
    private static final String MONGO_URI = "mongodb://mongodb:27017";
    private static final String DATABASE_NAME = "used_cars";
    private static final String COLLECTION_NAME = "cheap-listings";

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    public MongoDBWriter() {
        this.mongoClient = MongoClients.create(MONGO_URI);
        this.database = mongoClient.getDatabase(DATABASE_NAME);
        this.collection = database.getCollection(COLLECTION_NAME);
    }

    public void writeToMongo(String make, String model, int year, double price, String listingDate, String dealershipState, double lon, double lat, String vin, double percentageBelowAveragePrice) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date;
        try {
            date = formatter.parse(listingDate);

            Document doc = new Document()
                .append("make", make)
                .append("model", model)
                .append("year", year)
                .append("price", price)
                .append("dealership_state", dealershipState)
                .append("listing_date", date)
                .append("longitude", lon)
                .append("latitude", lat)
                .append("vin", vin)
                .append("percentage_below_average_price", percentageBelowAveragePrice);

            collection.insertOne(doc);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
