package com.zuhlke.ltd.camp.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
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
        final ArrayList<ReservationManager> mapReservationManagers = new ArrayList<ReservationManager>(ReservationManager.PARTITIONCOUNT);
        for(int i = 0; i < ReservationManager.PARTITIONCOUNT; i++) {
            final ReservationManager reservationManager = new ReservationManager(partitionsMap, 10000 + i);
            final int partition = reservationManager.reserve();
            assertThat(partition, is(equalTo(i)));
            mapReservationManagers.add(reservationManager);
        }
        final ReservationManager reservationManager = new ReservationManager(partitionsMap, ReservationManager.PARTITIONCOUNT);
        final int partition = reservationManager.reserve();
        assertThat(partition, is(equalTo(-1)));
    }


}