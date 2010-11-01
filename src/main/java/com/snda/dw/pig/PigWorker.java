package com.snda.dw.pig;

import java.io.ByteArrayInputStream;
import java.util.List;

import com.snda.dw.pig.listener.PigJobListener;

/**
 * Wrap each {@link PigJob} into PigWorker, add then you can add hook on the
 * execution life cycle of {@link PigJob}
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * @email zjffdu@gmail.com
 * 
 */
public class PigWorker extends Thread {

    private PigJob pigJob;

    private PigSession pigSession;

    private PigJobFuture future;

    public PigWorker(PigSession pigSession, PigJob pigJob) {
        this.pigSession = pigSession;
        this.pigJob = pigJob;
    }

    public void setPigJobFuture(PigJobFuture future) {
        this.future = future;
    }

    @Override
    public void run() {

        List<PigJobListener> listeners = pigJob.getListeners();
        try {
            // start
            for (PigJobListener listener : listeners) {
                listener.beforeStart(pigJob);
            }
            // run script
            PigServer2 pigServer = pigSession.getPigServerPool().getPigServer();
            this.pigJob.setPigServer(pigServer);
            pigServer.setJobName(pigJob.getJobName());
            pigServer.setJobPriority(pigJob.getPriority());
            pigServer.registerScript(new ByteArrayInputStream(pigJob
                    .getScript().getBytes("UTF-8")), pigJob.getParameters());
            // complete
            for (PigJobListener listener : listeners) {
                listener.onSucess(pigJob);
            }
            this.future.setSuccess();
        } catch (Throwable e) {
            // exception happens
            for (PigJobListener listener : listeners) {
                listener.onFailure(pigJob, e);
            }
            this.future.setFailure(e);
        } finally {
            this.future.setDone();
        }

    }
}
