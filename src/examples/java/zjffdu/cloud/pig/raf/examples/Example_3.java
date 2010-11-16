package zjffdu.cloud.pig.raf.examples;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.data.Tuple;

import zjffdu.cloud.pig.raf.PigConfiguration;
import zjffdu.cloud.pig.raf.PigJob;
import zjffdu.cloud.pig.raf.PigJobFuture;
import zjffdu.cloud.pig.raf.PigJobs;
import zjffdu.cloud.pig.raf.PigSession;
import zjffdu.cloud.pig.raf.extractor.ResultExtractor;
import zjffdu.cloud.pig.raf.listener.PigJobListenerAdapter;


public class Example_3 {

    private static class MyResultExtractor implements
            ResultExtractor<Map<String, Long>> {

        @Override
        public Map<String, Long> extract(Iterator<Tuple> iter)
                throws IOException {
            Map<String, Long> map = new HashMap<String, Long>();
            while (iter.hasNext()) {
                Tuple next = iter.next();
                map.put(next.get(0).toString(), Long.parseLong(next.get(1).toString()));
            }
            return map;
        }
    }

    public static void main(String[] args) {
        PigConfiguration conf = new PigConfiguration();
        PigSession session = new PigSession(conf);
        try {
            String input = "src/examples/data/input/example.txt";
            final String output = "src/examples/data/output_3";
            PigJob job = PigJobs
                    .newPigJobFromFile(
                            new File("src/examples/scripts/example_3.pig"))
                    .setJobName("example_1").setParameter("input", input)
                    .setParameter("output", output)
                    .setListener(new PigJobListenerAdapter() {

                        @Override
                        public void beforeStart(PigJob job) throws Exception {
                            // delete the destination folder before execution,
                            // this should be useful when you do local test
                            FileSystem fs = FileSystem.get(new Configuration());
                            fs.delete(new Path(output));
                        }

                        @Override
                        public void onSuccess(PigJob job) throws Exception {
                            // get the result from output path using the specified LoadFunc and ResultExtractor
                            Map<String, Long> result = job.getOutput(new Path(
                                    output), "PigStorage()",
                                    new MyResultExtractor());
                            for (Map.Entry<String, Long> entry : result
                                    .entrySet()) {
                                System.out.println(entry.getKey() + "\t"
                                        + entry.getValue());
                            }
                        }
                    });

            PigJobFuture future = session.submitPigJob(job);
            future.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            session.close();
        }
    }
}
