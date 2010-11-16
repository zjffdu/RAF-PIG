package zjffdu.cloud.pig.raf;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for one {@link PigSession}.
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * 
 */
public class PigConfiguration {


    public static enum Options {
        /**
         * Local or MapReduce, default value is Local
         */
        ExecType, 
        /**
         * The size of ThreadPool, default value is 1, because currently pig do not concurrent execute multpile pig script in one jvm
         */
        PoolSize, 
        /**
         * Udf jars you want to register to {@link PigServer}, if you have multiple jars,
         * separate them using colon. e.g.
         * <pre>
         * PigConfiguration conf=new PigConfiguration();
         * conf.set(Options.UDFJar,"lib/a.jar:lib/b.jar");
         * </pre>
         */
        UDFJar, 
        
        /**
         * UDF packages you want to import to {@link PigServer}, if you have multiple packages,
         * separate them using colon. e.g.
         * <pre>
         * PigConfiguration conf=new PigConfiguration();
         * conf.set(Options.UDFPackage,"com.tutorial.udf_1:com.tutorial.udf_2");
         * </pre>
         */
        UDFPackage,
    }

    private Map<Options, String> map = new HashMap<Options, String>();

    public PigConfiguration() {
        set(Options.ExecType, "Local"); // default ExecType is local
        set(Options.PoolSize, "1"); // default PoolSize is 1, because pig do not
                                    // support concurrent execution.
    }

    public String get(Options field) {
        return map.get(field) == null ? "" : map.get(field);
    }

    public void set(Options field, String value) {
        map.put(field, value);
    }

    public int getInt(Options field) {
        String value = get(field);
        if (value.trim().length() == 0) {
            return 0;
        } else {
            return Integer.parseInt(value);
        }
    }

    public static void main(String[] args) {
        PigConfiguration conf = new PigConfiguration();
        System.out.println(conf.getInt(Options.PoolSize));
    }
}
