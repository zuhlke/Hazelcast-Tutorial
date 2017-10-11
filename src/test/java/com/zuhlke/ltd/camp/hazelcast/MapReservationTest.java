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

    private HazelcastInstance hazelcastInstance;

    @Before
    public void setUp() {
        hazelcastInstance = Hazelcast.newHazelcastInstance(new Config());
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
        hazelcastInstance.shutdown();
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
    public void shouldReservePartition() throws IOException {
        final ReservationManager reservationManager = new ReservationManager(hazelcastInstance, "10000");
        final int partition = reservationManager.reserve();
        assertThat(partition, is(equalTo(0)));
    }

    @Test
    public void shouldNotReservePartitionIfTableFull() throws IOException {
        final int WORKER_COUNT = ReservationManager.PARTITION_COUNT;
        reserveAllPartitions(WORKER_COUNT);
        final ReservationManager reservationManager = new ReservationManager(hazelcastInstance, Integer.toString(10000 + WORKER_COUNT));
        final int partition = reservationManager.reserve();
        assertThat(partition, is(equalTo(-1)));
    }

    @Test
    public void shouldReservePartitionIfTableHasExpiredEntries() throws IOException {
        final int WORKER_COUNT = 5;
        reserveAllPartitions(WORKER_COUNT);
        final int midPartition = ReservationManager.PARTITION_COUNT / 2;
        invalidatePartition(midPartition);
        final ReservationManager reservationManager = new ReservationManager(hazelcastInstance, Integer.toString(10000 + WORKER_COUNT));
        final int partition = reservationManager.reserve();
        assertThat(partition, is(equalTo(midPartition)));
    }

    private void reserveAllPartitions(int workerCount) {
        final List<ReservationManager> reservationManagers = new ArrayList<>(workerCount);
        for(int i = 0; i < workerCount; i++) {
            final ReservationManager reservationManager = new ReservationManager(hazelcastInstance, Integer.toString(10000 + i));
            reservationManagers.add(reservationManager);
        }
        for(int i = 0; i < ReservationManager.PARTITION_COUNT; i++) {
            final ReservationManager reservationManager = reservationManagers.get(i % workerCount);
            final int partition = reservationManager.reserve();
            assertThat(partition, is(equalTo(i)));
        }
    }

    private void invalidatePartition(int key) {
        final String reservation;
        final TransactionContext context = hazelcastInstance.newTransactionContext();
        try {
            context.beginTransaction();
            final TransactionalMap partitionsMap = context.getMap(ReservationManager.PARTITION_MAP_NAME);
            reservation = (String) partitionsMap.get(key);
            final String expiredReservation = reservation.replaceFirst("(\"expiryTime\"\\:)\\d+", "$13600");
            partitionsMap.put(key, expiredReservation);
            context.commitTransaction();
        } catch (Throwable t) {
            context.rollbackTransaction();
            t.printStackTrace(System.err);
            fail("MapReservationTest.invalidatePartition() caught exception: " + t);
        }
    }

    @Test
    public void shouldUpdatePartition() throws IOException {
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

    @Test
    public void shouldCompletePartition() throws IOException {
        final int WORKER_COUNT = 4;
        reserveAllPartitions(WORKER_COUNT);
        final int firstPartition = 0;
        final int fourthPartition = 4;
        invalidatePartition(firstPartition);
        invalidatePartition(fourthPartition);
        final ReservationManager reservationManager = new ReservationManager(hazelcastInstance, "10000");
        final int partition = reservationManager.reserve();
        assertThat(partition, is(equalTo(firstPartition)));
        final Date newExpiryTime = reservationManager.markCompleted(partition);
        assertThat(newExpiryTime.getTime(), is(equalTo(-1L)));
        final int newPartition = reservationManager.reserve();
        assertThat(newPartition, is(equalTo(fourthPartition)));
    }
}