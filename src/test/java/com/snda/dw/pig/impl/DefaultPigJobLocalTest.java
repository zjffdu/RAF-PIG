package com.snda.dw.pig.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;

import com.google.common.io.Files;
import com.snda.dw.pig.PigConfiguration;
import com.snda.dw.pig.PigJob;
import com.snda.dw.pig.PigJobFuture;
import com.snda.dw.pig.PigJobs;
import com.snda.dw.pig.PigSession;
import com.snda.dw.pig.extractor.ResultExtractor;
import com.snda.dw.pig.listener.PigJobListenerAdapter;

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
        job.addListener(new PigJobListenerAdapter() {
            @Override
            public void onSucess(PigJob job) throws Exception {
                Map<String, Long> map = job.getOutput("c",
                        new MapResultExtractor_1());
                assertTrue(map.size() == 2);
                assertTrue(map.get("a").equals(3L));
                assertTrue(map.get("b").equals(5L));
            }
        });
        PigJobFuture future = session.submitPigJob(job);
        future.await();
        assertFalse(future.isCanceled());
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
        job.addListener(new PigJobListenerAdapter() {
            @Override
            public void onSucess(PigJob job) throws Exception {
                Map<String, Long> map = job.getOutput(new Path(outputPath),
                        "PigStorage()", new MapResultExtractor_2());
                assertTrue(map.size() == 2);
                assertTrue(map.get("a").equals(3L));
                assertTrue(map.get("b").equals(5L));
            }
        });
        PigJobFuture future = session.submitPigJob(job);
        future.await();
        assertFalse(future.isCanceled());
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
        assertFalse(future.isCanceled());
        assertTrue(future.isDone());
        assertFalse(future.isSuccess());
        assertEquals(FrontendException.class, future.getFailure().getClass());
    }

    // test cancel job
    public void testDefaultPigJob_4() throws IOException, InterruptedException {
        PigJob job = PigJobs.newPigJobFromClassPath("scripts/test_1.pig");
        job.setParameter("input", "src/test/resources/data/input/input_1.txt");
        job.addListener(new PigJobListenerAdapter() {

            @Override
            public void onSucess(PigJob job) throws Exception {
                Thread.currentThread().sleep(10 * 1000);
                Map<String, Long> map = job.getOutput("c",
                        new MapResultExtractor_1());
                assertTrue(map.size() == 2);
                assertTrue(map.get("a").equals(3L));
                assertTrue(map.get("b").equals(5L));
            }
        });
        PigJobFuture future = session.submitPigJob(job);
        future.cancel();

        assertFalse(future.isDone());
        assertFalse(future.isSuccess());
    }
}
