package com.zuhlke.ltd.camp.hazelcast;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Date;

public class MapReservation {
    private static final long WORK_INTERVAL_MILLIS = 5000; // 5 sec
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private Integer identity;
    private Date expiryTime;

    public static MapReservation fromJson(String representation) throws IOException {
        return objectMapper.readValue(representation, MapReservation.class);
    }

    public MapReservation(int identity) {
        this.identity = identity;
        this.expiryTime = new Date();
    }

    private MapReservation() {}

    public int getIdentity() {
        return identity;
    }

    public void setIdentity(int identity) {
        this.identity = identity;
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
}
