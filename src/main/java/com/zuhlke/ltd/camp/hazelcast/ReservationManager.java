package com.zuhlke.ltd.camp.hazelcast;

import java.io.IOException;
import java.util.Map;

class ReservationManager {
    public static final int PARTITIONCOUNT = 1000;

    private final Map<Integer, String> partitionsMap;
    private final MapReservation mapReservation;

    public ReservationManager(Map<Integer, String> partitionsMap, int identity) {
        this.partitionsMap = partitionsMap;
        this.mapReservation = new MapReservation(identity);
    }

    public int reserve() throws IOException {
        final int mapEntry = findVacantMapEntry();
        if (mapEntry >= 0) {
            this.mapReservation.setNewExpiryTime();
            partitionsMap.put(mapEntry, this.mapReservation.toJson());
        }
        return mapEntry;
    }

    private int findVacantMapEntry() {
        final int reservationCount = partitionsMap.size();
        if(reservationCount < PARTITIONCOUNT) {
            return reservationCount;
        }
        for(int i = 0; i < PARTITIONCOUNT; i++) {
            final String representation = partitionsMap.get(i);
            try {
                final MapReservation reservation = MapReservation.fromJson(representation);
                if (reservation.getExpiryTime().before(this.mapReservation.getExpiryTime())) {
                    return i;
                }
            } catch (IOException e) {
                // Not a valid entry
                return i;
            }
        }
        return -1;
    }
}
