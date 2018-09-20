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
        // Cloud Spanner �� ��ǰ �� hitCount�� ��ϵ� ���̺� 5�� ���� �޸𸮿� ���� hitCountMap�� ������Ʈ

        /// hitCountMap �ʱ�ȭ
        synchronized (hitCountMap) {
            hitCountMap.clear();
        }
    }
}
