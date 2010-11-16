package zjffdu.cloud.pig.raf;

import java.io.ByteArrayInputStream;
import java.util.List;

import zjffdu.cloud.pig.raf.listener.PigJobListener;


/**
 * Wrap each {@link PigJob} into PigWorker, add then you can add hook on the
 * execution life cycle of {@link PigJob}
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
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

        try {
            // start
            this.pigJob.getListener().beforeStart(pigJob);
            // run script
            PigServer2 pigServer = pigSession.getPigServerPool().getPigServer();
            this.pigJob.setPigServer(pigServer);
            pigServer.setJobName(pigJob.getJobName());
            pigServer.setJobPriority(pigJob.getPriority());
            pigServer.registerScript(new ByteArrayInputStream(pigJob
                    .getScript().getBytes("UTF-8")), pigJob.getParameters());
            // complete
            this.pigJob.getListener().onSuccess(pigJob);
            this.future.setSuccess();
        } catch (Throwable e) {
            // exception happens
            this.pigJob.getListener().onFailure(pigJob, e);
            this.future.setFailure(e);
        } finally {
            this.future.setDone();
        }

    }
}
