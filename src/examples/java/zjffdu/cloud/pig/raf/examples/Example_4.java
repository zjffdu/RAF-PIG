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


public class Example_4 {

    // Be cautious, you will notice that this ResultExtractor is a bit different from that one in Example_3,
    // The reason is that the output of Example_3 is stored using PigStorage which store data in plain text format, 
    // while here the output is store using BinStorage which is a internal storage format of pig, this storage that seriliazed the data type within data together,
    // so you do not need to convert string to the real types.
    private static class MyResultExtractor implements
            ResultExtractor<Map<String, Long>> {

        @Override
        public Map<String, Long> extract(Iterator<Tuple> iter)
                throws IOException {
            Map<String, Long> map = new HashMap<String, Long>();
            while (iter.hasNext()) {
                Tuple next = iter.next();
                map.put((String) next.get(0), (Long) (next.get(1)));
            }
            return map;
        }
    }

    public static void main(String[] args) {
        PigConfiguration conf = new PigConfiguration();
        PigSession session = new PigSession(conf);
        try {
            String input = "src/examples/data/input/example.txt";
            PigJob job = PigJobs
                    .newPigJobFromFile(
                            new File("src/examples/scripts/example_4.pig"))
                    .setJobName("example_1").setParameter("input", input)
                    .setListener(new PigJobListenerAdapter() {

                        @Override
                        public void onSuccess(PigJob job) throws Exception {
                            // get the result of alias using ResultExtractor
                            Map<String, Long> result = job.getOutput("d",
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
