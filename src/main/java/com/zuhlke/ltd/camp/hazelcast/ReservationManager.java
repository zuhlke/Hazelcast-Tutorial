package com.zuhlke.ltd.camp.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.transaction.TransactionContext;

import java.io.IOException;

class ReservationManager {
    public static final int PARTITION_COUNT = 1000;
    public static final String PARTITION_MAP_NAME = "partitions";

    //    private final TransactionContext context;
    private final HazelcastInstance hazelcastInstance;
    private final MapReservation mapReservation;

    public ReservationManager(HazelcastInstance hazelcastInstance, String workerId) {
        this.hazelcastInstance = hazelcastInstance;
        this.mapReservation = new MapReservation(workerId);
    }

    public synchronized int reserve() {
        final TransactionContext context = hazelcastInstance.newTransactionContext();
        try {
            context.beginTransaction();
            final TransactionalMap partitionsMap = context.getMap(PARTITION_MAP_NAME);
            final int mapEntry = findVacantMapEntry(partitionsMap);
//            System.out.println("Entry to be reserved: " + mapEntry);
            if (mapEntry >= 0) {
                mapReservation.setNewExpiryTime();
                partitionsMap.put(mapEntry, mapReservation.toJson());
            }
            context.commitTransaction();
            return mapEntry;
        } catch (Throwable t) {
            System.err.println("ReservationManager.reserve() caught exception: " + t);
            t.printStackTrace(System.err);
            context.rollbackTransaction();
            return -1;
        }
    }

    private int findVacantMapEntry(TransactionalMap partitionsMap) {
        final int reservationCount = partitionsMap.size();
        if (reservationCount < PARTITION_COUNT) {
            return reservationCount;
        }
        for (int i = 0; i < PARTITION_COUNT; i++) {
            final String representation = (String) partitionsMap.get(i);
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
