package zjffdu.cloud.pig.raf.impl;

import zjffdu.cloud.pig.raf.PigJobFuture;
import zjffdu.cloud.pig.raf.PigWorker;


/**
 * Default implementation for interface {@link PigJobFuture}
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 *
 */
public class DefaultPigJobFuture implements PigJobFuture {

    private volatile boolean isDone = false;

    private volatile boolean isSuccess = false;

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
    public boolean isDone() {
        return this.isDone;
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

}
