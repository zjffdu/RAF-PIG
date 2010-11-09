package com.snda.dw.pig;


/**
 * Returned object when you submit {@link PigJob} to {@link PigSession}, you can use this object to
 * track the execution progress of this {@link PigJob}
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 *
 */
public interface PigJobFuture {

    /**
     * Block method until the {@link PigJob} is completed
     * 
     * @throws InterruptedException
     */
    public void await() throws InterruptedException;

    public boolean isDone();

    void setDone();

    PigWorker getPigWorker();
    
    public boolean isSuccess();
    
    void setSuccess();
    
    /**
     * @return the cause exception of {@link PigJob}, return null if the {@link PigJob} execute successfully.
     */
    public Throwable getFailure();
    
    void setFailure(Throwable e);
}
