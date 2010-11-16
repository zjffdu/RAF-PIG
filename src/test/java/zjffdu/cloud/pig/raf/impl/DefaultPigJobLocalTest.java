package zjffdu.cloud.pig.raf.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;

import zjffdu.cloud.pig.raf.PigConfiguration;
import zjffdu.cloud.pig.raf.PigJob;
import zjffdu.cloud.pig.raf.PigJobFuture;
import zjffdu.cloud.pig.raf.PigJobs;
import zjffdu.cloud.pig.raf.PigSession;
import zjffdu.cloud.pig.raf.extractor.ResultExtractor;
import zjffdu.cloud.pig.raf.listener.PigJobListenerAdapter;

import com.google.common.io.Files;

public class DefaultPigJobLocalTest extends TestCase {

    private PigSession session;

    private static class MapResultExtractor_1 implements
            ResultExtractor<Map<String, Long>> {

        @Override
        public Map<String, Long> extract(Iterator<Tuple> iter)
                throws IOException {
            Map<String, Long> map = new HashMap<String, Long>();
            while (iter.hasNext()) {
                Tuple tuple = iter.next();
                map.put((String) tuple.get(0), (Long) tuple.get(1));
            }
            return map;
        }
    }

    private static class MapResultExtractor_2 implements
            ResultExtractor<Map<String, Long>> {

        @Override
        public Map<String, Long> extract(Iterator<Tuple> iter)
                throws IOException {
            Map<String, Long> map = new HashMap<String, Long>();
            while (iter.hasNext()) {
                Tuple tuple = iter.next();
                String key = tuple.get(0).toString();
                Long value = Long.parseLong(tuple.get(1).toString());
                map.put(key, value);
            }
            return map;
        }
    }

    @Override
    protected void setUp() throws Exception {
        session = new PigSession(new PigConfiguration());
    }

    @Override
    protected void tearDown() throws Exception {
        session.close();
    }

    // test points include parameter binding, get script file from class path,
    // get output from alias
    public void testDefaultPigJob_1() throws IOException, InterruptedException {
        PigJob job = PigJobs.newPigJobFromClassPath("scripts/test_1.pig");
        job.setParameter("input", "src/test/resources/data/input/input_1.txt");
        job.setListener(new PigJobListenerAdapter() {
            @Override
            public void onSuccess(PigJob job) throws Exception {
                Map<String, Long> map = job.getOutput("c",
                        new MapResultExtractor_1());
                assertTrue(map.size() == 2);
                assertTrue(map.get("a").equals(3L));
                assertTrue(map.get("b").equals(5L));
            }
        });
        PigJobFuture future = session.submitPigJob(job);
        future.await();
        assertTrue(future.isDone());
        assertTrue(future.isSuccess());
    }

    // test point include parameter binding and get output from path
    public void testDefaultPigJob_2() throws IOException, InterruptedException {
        PigJob job = PigJobs.newPigJobFromClassPath("scripts/test_2.pig");
        final String outputPath = "src/test/resources/data/output_1";
        File outputFile = new File(outputPath);
        if (outputFile.exists()) {
            Files.deleteRecursively(outputFile);
        }

        job.setParameter("input", "src/test/resources/data/input/input_1.txt")
                .setParameter("output", outputPath);
        job.setListener(new PigJobListenerAdapter() {
            @Override
            public void onSuccess(PigJob job) throws Exception {
                Map<String, Long> map = job.getOutput(new Path(outputPath),
                        "PigStorage()", new MapResultExtractor_2());
                assertTrue(map.size() == 2);
                assertTrue(map.get("a").equals(3L));
                assertTrue(map.get("b").equals(5L));
            }
        });
        PigJobFuture future = session.submitPigJob(job);
        future.await();
        assertTrue(future.isDone());
        assertTrue(future.isSuccess());
        outputFile.deleteOnExit();
    }

    // test incorrect pig script which will cause job fails
    public void testDefaultPigJob_3() throws IOException, InterruptedException {
        PigJob job = PigJobs
                .newPigJobFromClassPath("scripts/incorrect_script.pig");
        PigJobFuture future = session.submitPigJob(job);
        future.await();
        assertTrue(future.isDone());
        assertFalse(future.isSuccess());
        assertEquals(FrontendException.class, future.getFailure().getClass());
    }

}
