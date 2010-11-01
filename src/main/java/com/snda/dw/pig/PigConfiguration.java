package com.snda.dw.pig;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for one {@link PigSession}.
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * @email zjffdu@gmail.com 
 * 
 */
public class PigConfiguration {

    /**
     * 
     * @author Jeff Zhang
     * @email zjffdu@gmail.com
     * @blog http://zjffdu.blogspot.com
     * 
     */
    public static enum Options {
        ExecType, PoolSize, UDFJar, UDFPackage,
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
