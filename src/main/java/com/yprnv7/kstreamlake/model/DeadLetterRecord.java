package com.yprnv7.kstreamlake.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DeadLetterRecord {

    @JsonProperty("raw_payload")
    private String rawPayload;

    @JsonProperty("error_reason")
    private String errorReason;

    @JsonProperty("failed_at")
    private long failedAt;

    public DeadLetterRecord() {
    }

    public DeadLetterRecord(String rawPayload, String errorReason, long failedAt) {
        this.rawPayload = rawPayload;
        this.errorReason = errorReason;
        this.failedAt = failedAt;
    }

    public String getRawPayload() {
        return rawPayload;
    }

    public void setRawPayload(String rawPayload) {
        this.rawPayload = rawPayload;
    }

    public String getErrorReason() {
        return errorReason;
    }

    public void setErrorReason(String errorReason) {
        this.errorReason = errorReason;
    }

    public long getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(long failedAt) {
        this.failedAt = failedAt;
    }
}
