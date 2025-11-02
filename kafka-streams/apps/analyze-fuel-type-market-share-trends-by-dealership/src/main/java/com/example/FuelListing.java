package com.example;

public class FuelListing {
    private String dealership;
    private String fuelType;
    private String listingDate;

    public FuelListing() {}

    public FuelListing(String dealership, String fuelType, String listingDate) {
        this.dealership = dealership;
        this.fuelType = fuelType;
        this.listingDate = listingDate;
    }

    public String getDealership() {
        return dealership;
    }

    public String getFuelType() {
        return fuelType;
    }

    public String getListingDate() {
        return listingDate;
    }
}

