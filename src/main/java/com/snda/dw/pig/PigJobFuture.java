package com.snda.dw.pig;


/**
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * @email zjffdu@gmail.com 
 *
 */
public interface PigJobFuture {

    public void await() throws InterruptedException;

    public boolean isDone();

    void setDone();

    PigWorker getPigWorker();
    
    public boolean isSuccess();
    
    void setSuccess();
    
    public Throwable getFailure();
    
    void setFailure(Throwable e);
}
