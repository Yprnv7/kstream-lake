package com.yprnv7.kstreamlake.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class UserWindowAggregation {

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("window_start")
    private long windowStart;

    @JsonProperty("window_end")
    private long windowEnd;

    @JsonProperty("transaction_count")
    private long transactionCount;

    @JsonProperty("total_volume")
    private double totalVolume;

    @JsonProperty("max_transaction_amount")
    private double maxTransactionAmount;

    @JsonProperty("average_amount")
    private double averageAmount;

    @JsonProperty("anomaly_flag")
    private boolean anomalyFlag;

    public UserWindowAggregation() {
    }

    public UserWindowAggregation(String userId, long windowStart, long windowEnd, 
                                  long transactionCount, double totalVolume, 
                                  double maxTransactionAmount, double averageAmount, 
                                  boolean anomalyFlag) {
        this.userId = userId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.transactionCount = transactionCount;
        this.totalVolume = totalVolume;
        this.maxTransactionAmount = maxTransactionAmount;
        this.averageAmount = averageAmount;
        this.anomalyFlag = anomalyFlag;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public long getTransactionCount() {
        return transactionCount;
    }

    public void setTransactionCount(long transactionCount) {
        this.transactionCount = transactionCount;
    }

    public double getTotalVolume() {
        return totalVolume;
    }

    public void setTotalVolume(double totalVolume) {
        this.totalVolume = totalVolume;
    }

    public double getMaxTransactionAmount() {
        return maxTransactionAmount;
    }

    public void setMaxTransactionAmount(double maxTransactionAmount) {
        this.maxTransactionAmount = maxTransactionAmount;
    }

    public double getAverageAmount() {
        return averageAmount;
    }

    public void setAverageAmount(double averageAmount) {
        this.averageAmount = averageAmount;
    }

    public boolean isAnomalyFlag() {
        return anomalyFlag;
    }

    public void setAnomalyFlag(boolean anomalyFlag) {
        this.anomalyFlag = anomalyFlag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserWindowAggregation that = (UserWindowAggregation) o;
        return windowStart == that.windowStart &&
                windowEnd == that.windowEnd &&
                transactionCount == that.transactionCount &&
                Double.compare(that.totalVolume, totalVolume) == 0 &&
                Double.compare(that.maxTransactionAmount, maxTransactionAmount) == 0 &&
                Double.compare(that.averageAmount, averageAmount) == 0 &&
                anomalyFlag == that.anomalyFlag &&
                Objects.equals(userId, that.userId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, windowStart, windowEnd, transactionCount, totalVolume, maxTransactionAmount, averageAmount, anomalyFlag);
    }
}
