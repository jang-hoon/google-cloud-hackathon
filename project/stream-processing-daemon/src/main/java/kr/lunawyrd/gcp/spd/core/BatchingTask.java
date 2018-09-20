package kr.lunawyrd.gcp.spd.core;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchingTask extends Thread {

    private Map<String, AtomicInteger> hitCountMap;
    private long period;
    private long beforeTime;

    public BatchingTask(Map<String, AtomicInteger> hitCountMap) {
        this.hitCountMap = hitCountMap;
        this.period = 5 * 60 * 1000;
    }

    @Override
    public void run() {
        try {
            while(true) {
                long currentTime = System.currentTimeMillis();
                if((currentTime - beforeTime) > period) {
                    process();
                    beforeTime = currentTime;
                }

                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public void process() {
        // Cloud Spanner 에 상품 별 hitCount가 기록된 테이블에 5분 동안 메모리에 모은 hitCountMap을 업데이트

        /// hitCountMap 초기화
        synchronized (hitCountMap) {
            hitCountMap.clear();
        }
    }
}
