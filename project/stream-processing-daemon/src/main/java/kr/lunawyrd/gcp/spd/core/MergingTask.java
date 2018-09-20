package kr.lunawyrd.gcp.spd.core;

import com.google.cloud.datastore.*;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MergingTask extends Thread {

    private Map<String, AtomicInteger> hitCountMap;
    private long period;
    private long beforeTime;

    public MergingTask(Map<String, AtomicInteger> hitCountMap) {
        this.hitCountMap = hitCountMap;
        this.period = 10 * 1000;
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
        // Cloud Spanner���� ��ǰ �� hitCount�� ��ϵ� ���̺� ��ȸ
        // hitCountMap�� �ִ� �ǽð� hitConunt�� Merge
        // �ֻ��� 20�� ����
        // Cloud Spanner �ֵ� ���̺� ������Ʈ
    }

}
