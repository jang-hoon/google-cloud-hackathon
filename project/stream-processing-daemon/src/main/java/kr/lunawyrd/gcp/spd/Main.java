package kr.lunawyrd.gcp.spd;

import kr.lunawyrd.gcp.spd.core.PubSubSubscriber;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello, Stream Processing Daemon!");

        try {
            StreamProcessingDaemon streamProcessingDaemon = new StreamProcessingDaemon();
            streamProcessingDaemon.start();
            streamProcessingDaemon.pending();
        }catch (Throwable t){
            t.printStackTrace();
        }
    }
}
