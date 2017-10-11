package com.zuhlke.ltd.camp.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Map;

public class FederatedTask {

    public static final int PARTITIONCOUNT = 1000;
    private static Map<Integer, String> partitionsMap;

    public static void main(String[] args) {
        Config cfg = new Config();
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
        partitionsMap = instance.getMap("partitions");
        partitionsMap.put(1, "Joe");
        partitionsMap.put(2, "Ali");
        partitionsMap.put(3, "Avi");

        System.out.println("Customer with key 1: "+ partitionsMap.get(1));
        System.out.println("Map Size:" + partitionsMap.size());
    }
}
