package com.snda.dw.pig;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.snda.dw.pig.impl.DefaultPigJob;

/**
 * Utility and factory class for {@link PigJob}, try to create {@link PigJob}
 * using this class.
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * @email zjffdu@gmail.com 
 * 
 */
public class PigJobs {

    /**
     * 
     * @param script
     * @return
     * @throws IOException
     */
    public static PigJob newPigJob(String script) throws IOException {
        return new DefaultPigJob(new ByteArrayInputStream(
                script.getBytes("UTF-8")));
    }

    /**
     * 
     * @param scriptFile
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static PigJob newPigJobFromFile(File scriptFile)
            throws FileNotFoundException, IOException {
        return new DefaultPigJob(new FileInputStream(scriptFile));
    }

    /**
     * 
     * @param in
     * @return
     * @throws IOException
     */
    public static PigJob newPigJob(InputStream in) throws IOException {
        return new DefaultPigJob(in);
    }

    /**
     * 
     * @param resource
     * @return
     * @throws IOException
     */
    public static PigJob newPigJobFromClassPath(String resource)
            throws IOException {
        InputStream in = PigJobs.class.getClassLoader().getResourceAsStream(
                resource);
        return newPigJob(in);
    }
}
