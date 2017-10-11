package com.zuhlke.ltd.camp.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.transaction.TransactionContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MapReservationTest {

    private final Config cfg = new Config();
    private final HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(cfg);

    @Before
    public void setUp() {
        final TransactionContext context = hazelcastInstance.newTransactionContext();
        try {
            context.beginTransaction();
            final TransactionalMap partitionsMap = context.getMap(ReservationManager.PARTITION_MAP_NAME);
            partitionsMap.keySet().forEach(partitionsMap::remove);
            context.commitTransaction();
        } catch (Throwable t) {
            context.rollbackTransaction();
            t.printStackTrace(System.err);
            fail("MapReservationTest.setup() caught exception: " + t);
        }
    }

    @After
    public void tearDown() {

    }

    @Test
    public void shouldCreateMap() {
        final TransactionContext context = hazelcastInstance.newTransactionContext();
        try {
            context.beginTransaction();
            final TransactionalMap partitionsMap = context.getMap(ReservationManager.PARTITION_MAP_NAME);
            assertThat(partitionsMap.size(), is(equalTo(0)));
        } finally {
            context.rollbackTransaction();
        }
    }

    @Test
    public void shouldReserveEntry() throws IOException {
        final ReservationManager reservationManager = new ReservationManager(hazelcastInstance, "10000");
        final int partition = reservationManager.reserve();
        assertThat(partition, is(equalTo(0)));
    }

    @Test
    public void shouldNotReserveEntryIfTableFull() throws IOException {
        for(int i = 0; i < ReservationManager.PARTITION_COUNT; i++) {
            final ReservationManager reservationManager = new ReservationManager(hazelcastInstance, Integer.toString(10000 + i));
            final int partition = reservationManager.reserve();
//            assertThat(partition, is(equalTo(i)));
        }
        final ReservationManager reservationManager = new ReservationManager(hazelcastInstance, Integer.toString(10000 + ReservationManager.PARTITION_COUNT));
        final int partition = reservationManager.reserve();
        assertThat(partition, is(equalTo(-1)));
    }

    @Test
    public void shouldReserveEntryIfTableHasExpiredEntries() throws IOException {
        final int WORKER_COUNT = 5;
        final List<ReservationManager> reservationManagers = new ArrayList<>(WORKER_COUNT);
        for(int i = 0; i < WORKER_COUNT; i++) {
            final ReservationManager reservationManager = new ReservationManager(hazelcastInstance, Integer.toString(10000 + i));
            reservationManagers.add(reservationManager);
        }
        for(int i = 0; i < ReservationManager.PARTITION_COUNT; i++) {
            final ReservationManager reservationManager = reservationManagers.get(i % WORKER_COUNT);
            final int partition = reservationManager.reserve();
            assertThat(partition, is(equalTo(i)));
        }
        final int midKey = ReservationManager.PARTITION_COUNT / 2;
        invalidateMapEntry(midKey);
        final ReservationManager reservationManager = new ReservationManager(hazelcastInstance, Integer.toString(10000 + WORKER_COUNT));
        final int partition = reservationManager.reserve();
        assertThat(partition, is(equalTo(midKey)));
    }

    private String invalidateMapEntry(int key) {
        final String reservation;
        final TransactionContext context = hazelcastInstance.newTransactionContext();
        try {
            context.beginTransaction();
            final TransactionalMap partitionsMap = context.getMap(ReservationManager.PARTITION_MAP_NAME);
            reservation = (String) partitionsMap.get(key);
            final String expiredReservation = reservation.replaceFirst("(\"expiryTime\"\\:)\\d+", "$13600");
            partitionsMap.put(key, expiredReservation);
            context.commitTransaction();
            return reservation;
        } catch (Throwable t) {
            context.rollbackTransaction();
            t.printStackTrace(System.err);
            fail("MapReservationTest.invalidateMapEntry() caught exception: " + t);
        }
        return null;
    }
    
    @Test
    public void shouldUpdateEntry() throws IOException {
        final ReservationManager reservationManager = new ReservationManager(hazelcastInstance, "10000");
        final int partition = reservationManager.reserve();
        assertThat(partition, is(equalTo(0)));
        final Date originalExpiryTime = new Date(reservationManager.getExpiryTime().getTime());
        try {
            Thread.sleep(20L);
        } catch (InterruptedException ignored) {
        }
        final Date newExpiryTime = reservationManager.refreshTimestamp(partition);
        assertTrue("New expiry time " + newExpiryTime + " should be greater than " + originalExpiryTime,
                newExpiryTime.after(originalExpiryTime));
    }
}