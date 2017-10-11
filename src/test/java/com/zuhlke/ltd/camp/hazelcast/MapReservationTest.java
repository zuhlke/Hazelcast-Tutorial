package com.zuhlke.ltd.camp.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MapReservationTest {

    private final Config cfg = new Config();
    private final HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
    private final Map<Integer, String> partitionsMap = instance.getMap("partitions");

    @Before
    public void setUp() {
        partitionsMap.clear();
    }

    @After
    public void tearDown() {
        partitionsMap.clear();
    }

    @Test
    public void shouldCreateMap() {
        assertThat(partitionsMap.size(), is(equalTo(0)));
    }

    @Test
    public void shouldReserveEntry() throws IOException {
        final ReservationManager reservationManager = new ReservationManager(partitionsMap, 10000);
        final int partition = reservationManager.reserve();
        assertThat(partition, is(equalTo(0)));
    }

    @Test
    public void shouldNotReserveEntryIfTableFull() throws IOException {
        for(int i = 0; i < ReservationManager.PARTITION_COUNT; i++) {
            final ReservationManager reservationManager = new ReservationManager(partitionsMap, 10000 + i);
            final int partition = reservationManager.reserve();
            assertThat(partition, is(equalTo(i)));
        }
        final ReservationManager reservationManager = new ReservationManager(partitionsMap, ReservationManager.PARTITION_COUNT);
        final int partition = reservationManager.reserve();
        assertThat(partition, is(equalTo(-1)));
    }

    @Test
    public void shouldReserveEntryIfTableHasExpiredEntries() throws IOException {
        final int WORKER_COUNT = 5;
        final List<ReservationManager> reservationManagers = new ArrayList<ReservationManager>(WORKER_COUNT);
        for(int i = 0; i < WORKER_COUNT; i++) {
            final ReservationManager reservationManager = new ReservationManager(partitionsMap, 10000 + i);
            reservationManagers.add(reservationManager);
        }
        for(int i = 0; i < ReservationManager.PARTITION_COUNT; i++) {
            final ReservationManager reservationManager = reservationManagers.get(i % WORKER_COUNT);
            final int partition = reservationManager.reserve();
            assertThat(partition, is(equalTo(i)));
        }
        final int midKey = ReservationManager.PARTITION_COUNT / 2;
        final String midReservation = partitionsMap.get(midKey);
        final String expiredMidReservation = midReservation.replaceFirst("(\"expiryTime\"\\:)\\d+", "$13600");
        partitionsMap.put(midKey, expiredMidReservation);
        final ReservationManager reservationManager = new ReservationManager(partitionsMap, ReservationManager.PARTITION_COUNT);
        final int partition = reservationManager.reserve();
        assertThat(partition, is(equalTo(midKey)));
    }
}