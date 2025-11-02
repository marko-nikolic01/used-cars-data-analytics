package com.example;

public class FuelMarketShare {
    private long dealerCount;
    private long fuelTotal;
    private double share;

    public FuelMarketShare() {}

    public FuelMarketShare(long dealerCount, long fuelTotal, double share) {
        this.dealerCount = dealerCount;
        this.fuelTotal = fuelTotal;
        this.share = share;
    }

    public long getDealerCount() {
        return dealerCount;
    }

    public long getFuelTotal() {
        return fuelTotal;
    }

    public double getShare() {
        return share;
    }
}
