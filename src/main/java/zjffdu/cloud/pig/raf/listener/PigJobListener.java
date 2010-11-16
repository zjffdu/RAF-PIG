package zjffdu.cloud.pig.raf.listener;

import zjffdu.cloud.pig.raf.PigJob;

/**
 * 
 * Hook interface for the execution life-cycle of {@link PigJob}
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
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
     * This method is called after the pig script is registered through {@link PigSevrver}, but do not mean the mapreduce job is done.
     * Because there's two kinds of pig script, one kind of pig script having dump or store statement which will generate mapreduce jobs., while the
     * other kind of pig script is without dump and store statement, the mapreduce jobs is executed until you call method PigServer.openIterator()
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
