package com.zuhlke.ltd.camp.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Map;

public class FederatedTask {

    private static Map<Integer, String> partitionsMap;

    public static void main(String[] args) {
        Config cfg = new Config();
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
        partitionsMap = instance.getMap("partitions");
    }
}
