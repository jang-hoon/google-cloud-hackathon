package kr.lunawyrd.gcp.spd;

import kr.lunawyrd.gcp.spd.core.BatchingTask;
import kr.lunawyrd.gcp.spd.core.MergingTask;
import kr.lunawyrd.gcp.spd.core.PubSubSubscriber;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamProcessingDaemon {

    private boolean started;
    private Map<String, AtomicInteger> hitCountMap;

    public synchronized void start() throws Throwable{
        if(started) {
            throw new IllegalStateException();
        }

        hitCountMap = new ConcurrentHashMap<>();

        PubSubSubscriber pubSubSubscriber = new PubSubSubscriber("", "", hitCountMap);
        pubSubSubscriber.start();

        BatchingTask batchingTask = new BatchingTask(hitCountMap);
        batchingTask.start();

        MergingTask mergingTask = new MergingTask(hitCountMap);
        mergingTask.start();

        started = true;
    }

    public void pending() {
        while(started) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
