package com.zuhlke.ltd.camp.hazelcast;

import java.util.Map;

public class MapReservation {
    private final Map<Integer, String> partitionsMap;

    public MapReservation(Map<Integer, String> partitionsMap) {
        this.partitionsMap = partitionsMap;
    }
}
