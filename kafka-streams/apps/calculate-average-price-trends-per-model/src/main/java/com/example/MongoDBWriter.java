package com.example;

import com.mongodb.client.*;
import org.bson.Document;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MongoDBWriter {
    private static final String MONGO_URI = "mongodb://mongodb:27017";
    private static final String DATABASE_NAME = "used_cars";
    private static final String CAR_LISTINGS_COLLECTION_NAME = "car_listings";
    private static final String AVERAGE_PRICES_COLLECTION_NAME = "average_price_trends_per_model";

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> carListingsCollection;
    private MongoCollection<Document> averagePricesCollection;

    public MongoDBWriter() {
        this.mongoClient = MongoClients.create(MONGO_URI);
        this.database = mongoClient.getDatabase(DATABASE_NAME);
        this.carListingsCollection = database.getCollection(CAR_LISTINGS_COLLECTION_NAME);
        this.averagePricesCollection = database.getCollection(AVERAGE_PRICES_COLLECTION_NAME);
    }

    public void writeListing(String make, String model, int year, double price, String listingDate, String dealershipState, double lon, double lat, String vin) {
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
                .append("vin", vin);

            carListingsCollection.insertOne(doc);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        
    }

    public void writeAverage(String make, String model, int year, double avgPrice, Date timestamp) {
        Document doc = new Document()
            .append("make", make)
            .append("model", model)
            .append("year", year)
            .append("avg_price", avgPrice)
            .append("timestamp", timestamp);

        averagePricesCollection.insertOne(doc);
    }
}
