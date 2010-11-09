package com.snda.dw.pig.listener;

import com.snda.dw.pig.PigJob;

/**
 * 
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * @email zjffdu@gmail.com 
 * 
 */
public interface PigJobListener {

    /**
     * Do stuff before register the pig script
     * 
     * @param job
     * @throws Exception
     */
    public void beforeStart(final PigJob job) throws Exception;

    /**
     * Do stuff when you register pig script successfully. Be cautious here that
     * there's two kinds of pig script, one kind of pig script you get the
     * output by using pig statement, e.g.having using dump or store. In the
     * other kind of pig script you get the output by using java api, e.g.
     * PigServer.openIterator() in the this method.
     * 
     * @param job
     * @throws Exception
     */
    public void onSuccess(final PigJob job) throws Exception;

    /**
     * Do stuff when exception happens.
     * 
     * @param job
     * @param e
     */
    public void onFailure(final PigJob job, Throwable e);
}
