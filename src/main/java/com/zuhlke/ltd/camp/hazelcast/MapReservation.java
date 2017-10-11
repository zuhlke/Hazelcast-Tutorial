package com.zuhlke.ltd.camp.hazelcast;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Date;

public class MapReservation {
    private static final long WORK_INTERVAL_MILLIS = 5000L; // 5 sec
    private static final long FIDDLE_FACTOR_MILLIS = 200L; // Allow for clock disparity between instances
    private static final long COMPLETED_TIMESTAMP  = -1L;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private String workerId;
    private Date expiryTime;

    public static MapReservation fromJson(String representation) throws IOException {
        return objectMapper.readValue(representation, MapReservation.class);
    }

    public MapReservation(String workerId) {
        this.workerId = workerId;
        this.expiryTime = new Date();
    }

    private MapReservation() {}

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public Date getExpiryTime() {
        return expiryTime;
    }

    public void setExpiryTime(Date expiryTime) {
        this.expiryTime = expiryTime;
    }

    public String toJson() throws IOException {
        StringWriter writer = new StringWriter();
        objectMapper.writeValue(writer, this);
        return writer.toString();
    }

    public void setNewExpiryTime() {
        expiryTime.setTime(System.currentTimeMillis() + WORK_INTERVAL_MILLIS);
    }

    public Date computeExpiryTimeLimit() {
        return new Date(System.currentTimeMillis() + FIDDLE_FACTOR_MILLIS);
    }

    public void markCompleted() {
        expiryTime.setTime(COMPLETED_TIMESTAMP);
    }

    public boolean notCompleted() {
        return (expiryTime.getTime() != COMPLETED_TIMESTAMP);
    }
}
