package com.example;

public class ModelMarketShare {
    private String make;
    private String model;
    private int year;
    private String priceRange;
    private long modelCount;
    private long priceRangeTotal;
    private double marketShare;
    
    public ModelMarketShare() {}

    public ModelMarketShare(String make, String model, int year, String priceRange, long modelCount, long priceRangeTotal, double marketShare) {
        this.make = make;
        this.model = model;
        this.year = year;
        this.priceRange = priceRange;
        this.modelCount = modelCount;
        this.priceRangeTotal = priceRangeTotal;
        this.marketShare = marketShare;
    }

    public String getMake() { return make; }
    public String getModel() { return model; }
    public int getYear() { return year; }
    public String getPriceRange() { return priceRange; }
    public long getModelCount() { return modelCount; }
    public long getPriceRangeTotal() { return priceRangeTotal; }
    public double getMarketShare() { return marketShare; }
}
