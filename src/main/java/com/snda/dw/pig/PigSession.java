package com.snda.dw.pig;

import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.fs.Path;
import org.apache.pig.data.Tuple;

import com.snda.dw.pig.PigConfiguration.Options;
import com.snda.dw.pig.impl.DefaultPigJobFuture;
import com.snda.dw.pig.listener.PigJobListenerAdapter;

/**
 * This class represents one session to PigServer, this is the only way to
 * interact with Pig such as submitting @link {@link PigJob} using this class.
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * 
 */
public class PigSession {

    private PigServer2Factory pigServerPool;

    private PigConfiguration conf;

    private ExecutorService executor;

    public PigSession(PigConfiguration conf) {
        this.conf = conf;
        pigServerPool = new PigServer2Factory(conf);
        executor = Executors.newFixedThreadPool(conf.getInt(Options.PoolSize));
        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                PigSession.this.close();
            }
        });
    }

    PigServer2Factory getPigServerPool() {
        return this.pigServerPool;
    }

    /**
     * This method return immediately, you can use the returned object {@link PigJobFuture} to track the progress of {@link PigJob}
     * 
     * @param job
     * @return
     */
    public PigJobFuture submitPigJob(PigJob job) {
        PigWorker worker = new PigWorker(this, job);
        PigJobFuture future = new DefaultPigJobFuture(worker);
        executor.submit(worker);
        return future;
    }

    /**
     * Wait until all the submitted {@link PigJob} completed, remember to call this method at the end your program. The best practice is to put in the finally block.
     */
    public void close() {
        this.executor.shutdown();
    }

    public static void main(String[] args) throws IOException {
        PigConfiguration conf = new PigConfiguration();
        PigSession session = new PigSession(conf);
        PigJob job = PigJobs.newPigJobFromFile(new File("scripts/Test.pig"))
                .setJobName("test").setPriority("normal")
                .setParameter("input", "data/input.txt")
                .setListener(new PigJobListenerAdapter() {

                    @Override
                    public void beforeStart(PigJob job) throws Exception {
                        System.out.println("Start");
                    }

                    @Override
                    public void onSuccess(PigJob job) throws Exception {
                        Iterator<Tuple> iter = job.getOutput(new Path(
                                "data/output"), "PigStorage()");
                        while (iter.hasNext()) {
                            System.out.println(iter.next());
                        }
                    }
                });

        session.submitPigJob(job);
        session.close();
    }
}
