package com.snda.dw.pig.impl;

import com.snda.dw.pig.PigJobFuture;
import com.snda.dw.pig.PigWorker;


/**
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * @email zjffdu@gmail.com 
 *
 */
public class DefaultPigJobFuture implements PigJobFuture {

    private volatile boolean isDone = false;

    private volatile boolean isSuccess = false;

    private volatile boolean isCancaled = false;

    private Throwable cause;

    private PigWorker worker;

    public DefaultPigJobFuture(PigWorker worker) {
        this.worker = worker;
        this.worker.setPigJobFuture(this);
    }

    @Override
    public void await() throws InterruptedException {
        while (!isDone) {
            Thread.sleep(1000);
        }
        setDone();
    }

    @Override
    public void await(long milliSeconds) {

    }

    @Override
    public boolean isDone() {
        return this.isDone;
    }

    @Override
    public void cancel() {
        if (isDone) {
            return ;
        }
        synchronized (this) {
            this.isCancaled = true;
            worker.interrupt();
        }
    }

    @Override
    public void setDone() {
        this.isDone = true;
    }

    @Override
    public PigWorker getPigWorker() {
        return this.worker;
    }

    @Override
    public boolean isSuccess() {
        return this.isSuccess;
    }

    @Override
    public void setSuccess() {
        this.isSuccess = true;
    }

    @Override
    public Throwable getFailure() {
        return this.cause;
    }

    @Override
    public void setFailure(Throwable e) {
        this.isSuccess = false;
        this.cause = e;
    }

    @Override
    public boolean isCanceled() {
        return isCancaled;
    }

}
