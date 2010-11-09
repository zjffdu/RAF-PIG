package com.snda.dw.pig.examples;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.snda.dw.pig.PigConfiguration;
import com.snda.dw.pig.PigJob;
import com.snda.dw.pig.PigJobFuture;
import com.snda.dw.pig.PigJobs;
import com.snda.dw.pig.PigSession;
import com.snda.dw.pig.listener.PigJobListenerAdapter;

// Asynchronously execute pig script

public class Example_1 {

    public static void main(String[] args) {
        
        PigConfiguration conf = new PigConfiguration();
        PigSession session = new PigSession(conf);
        try {
            final String input = "src/examples/data/input/example.txt";
            final String output = "src/examples/data/output_1";
            PigJob job = PigJobs
                    .newPigJobFromFile(
                            new File("src/examples/scripts/example_1.pig"))
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
                            // print success message
                            System.out.println("Exeucte pig job sucessfully:\n"
                                    + job.getScript());
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
