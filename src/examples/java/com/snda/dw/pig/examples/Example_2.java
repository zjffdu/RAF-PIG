package com.snda.dw.pig.examples;

import java.io.File;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.data.Tuple;

import com.snda.dw.pig.PigConfiguration;
import com.snda.dw.pig.PigJob;
import com.snda.dw.pig.PigJobFuture;
import com.snda.dw.pig.PigJobs;
import com.snda.dw.pig.PigSession;
import com.snda.dw.pig.listener.PigJobListenerAdapter;

public class Example_2 {
    
    public static void main(String[] args) {
        
        PigConfiguration conf = new PigConfiguration();
        PigSession session = new PigSession(conf);
        try {
            String input = "src/examples/data/input/example.txt";
            PigJob job = PigJobs
                    .newPigJobFromFile(
                            new File("src/examples/scripts/example_2.pig"))
                    .setJobName("example_1").setParameter("input", input)
                    .setListener(new PigJobListenerAdapter() {

                        @Override
                        public void onSuccess(PigJob job) throws Exception {
                            // get the result of alias
                            Iterator<Tuple> iter = job.getOutput("d");
                            while (iter.hasNext()) {
                                System.out.println(iter.next());
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
