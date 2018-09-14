package kr.lunawyrd.gcp.spd;

import kr.lunawyrd.gcp.spd.core.PubSubSubscriber;

public class StreamProcessingDaemon {

    private boolean started;

    public synchronized void start() throws Throwable{
        if(started) {
            throw new IllegalStateException();
        }

        PubSubSubscriber pubSubSubscriber = new PubSubSubscriber("", "");
        pubSubSubscriber.start();

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
