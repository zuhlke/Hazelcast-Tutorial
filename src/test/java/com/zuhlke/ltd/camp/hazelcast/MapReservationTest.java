package com.zuhlke.ltd.camp.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.*;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MapReservationTest {

    private final Config cfg = new Config();
    private final HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
    private Map<Object, Object> partitionsMap;

    @Before
    public void setUp() {
        partitionsMap = instance.getMap("partitions");
    }

    @After
    public void tearDown() {
    }

    @Test
    public void shouldCreateMap() {
        assertThat(partitionsMap.size(), is(equalTo(0)));
    }
}