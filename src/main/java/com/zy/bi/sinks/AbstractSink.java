package com.zy.bi.sinks;

/**
 * Created by allen on 2015/8/28.
 */
public abstract class AbstractSink {

    abstract void process();

    public abstract void configure(String confFile);

    public synchronized void start() {}

    public synchronized void stop() {}

    public static enum Status {
        READY, BACKOFF
    }

}
