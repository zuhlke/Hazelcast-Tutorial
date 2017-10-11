package com.zuhlke.ltd.camp.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.io.IOException;

public class FederatedTask {

    private static ReservationManager reservationManager;
    private static String workerId;

    public static void main(String[] args) {
        Config cfg = new Config();
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
        workerId = instance.getLocalEndpoint().getUuid();
        reservationManager = new ReservationManager(instance, workerId);
        try {
            workUntilNoMoreTasks();
        } catch (IOException e) {
            e.printStackTrace();
        }
        instance.shutdown();
    }

    private static void workUntilNoMoreTasks() throws IOException {
        while(true) {
            final int partition = reservationManager.reserve();
            if(partition < 0) {
                return;
            }
            System.out.println("Starting work on partition no. " + partition);

        }
    }
}
