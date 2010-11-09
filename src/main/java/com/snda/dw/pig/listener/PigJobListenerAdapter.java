package com.snda.dw.pig.listener;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.snda.dw.pig.PigJob;

/**
 * Adapter class for user's easy user of interface {@link PigJobListener}.
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * 
 */
public class PigJobListenerAdapter implements PigJobListener {

    private static Logger LOGGER = LoggerFactory
            .getLogger(PigJobListener.class);

    @Override
    public void beforeStart(final PigJob job) throws Exception {
        LOGGER.info("Start for pig script:\n" + job.getScript());
    }

    @Override
    public void onSuccess(final PigJob job) throws Exception {
        LOGGER.info("Run successfully for pig script:\n" + job.getScript());
    }

    @Override
    public void onFailure(final PigJob job, Throwable e) {
        try {
            LOGGER.error("Run failure for pig script:\n" + job.getScript());
            LOGGER.error(Throwables.getStackTraceAsString(e));
        } catch (IOException e1) {
            LOGGER.error(Throwables.getStackTraceAsString(e1));
        }
    }

}
