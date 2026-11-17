package com.yprnv7.kstreamlake.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TransactionEvent {

    @JsonProperty("transaction_id")
    private String transactionId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("merchant_id")
    private String merchantId;

    @JsonProperty("amount")
    private double amount;

    @JsonProperty("currency")
    private String currency;

    @JsonProperty("country")
    private String country;

    @JsonProperty("timestamp")
    private long timestamp;

    public TransactionEvent() {
    }

    public TransactionEvent(String transactionId, String userId, String merchantId, 
                            double amount, String currency, String country, long timestamp) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.merchantId = merchantId;
        this.amount = amount;
        this.currency = currency;
        this.country = country;
        this.timestamp = timestamp;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isValid() {
        return transactionId != null && !transactionId.isBlank()
                && userId != null && !userId.isBlank()
                && amount > 0.0
                && currency != null && !currency.isBlank()
                && timestamp > 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionEvent that = (TransactionEvent) o;
        return Double.compare(that.amount, amount) == 0 &&
                timestamp == that.timestamp &&
                Objects.equals(transactionId, that.transactionId) &&
                Objects.equals(userId, that.userId) &&
                Objects.equals(merchantId, that.merchantId) &&
                Objects.equals(currency, that.currency) &&
                Objects.equals(country, that.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, userId, merchantId, amount, currency, country, timestamp);
    }

    @Override
    public String toString() {
        return "TransactionEvent{" +
                "transactionId='" + transactionId + '\'' +
                ", userId='" + userId + '\'' +
                ", merchantId='" + merchantId + '\'' +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                ", country='" + country + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
