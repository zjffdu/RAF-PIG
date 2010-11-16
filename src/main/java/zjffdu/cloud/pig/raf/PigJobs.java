package zjffdu.cloud.pig.raf;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import zjffdu.cloud.pig.raf.impl.DefaultPigJob;


/**
 * Utility and factory class for {@link PigJob}, try to create {@link PigJob}
 * using this class.
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * 
 */
public class PigJobs {

    /**
     * Create PigJob from a in-memory pig script
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
     * Create PigJob from a pig script file
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
     * Create PigJob from {@link InputStream}, such as {@link SocketInputStream} which means the pig script is on a remote machine
     * 
     * @param in
     * @return
     * @throws IOException
     */
    public static PigJob newPigJob(InputStream in) throws IOException {
        return new DefaultPigJob(in);
    }

    /**
     * Create PigJob from a resource file on classpath
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
