package com.snda.dw.pig.util;

import java.io.IOException;

import org.apache.pig.data.Tuple;

/**
 * Factory class for {@link TupleFormat}
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * 
 */
public class TupleFormatFactory {

    /**
     * Create a tuple with specified <code>delimiter</code>
     * 
     * @param delimiter
     * @return
     */
    public static TupleFormat on(final String delimiter) {
        return new TupleFormat() {
            @Override
            public String format(Tuple tuple) throws IOException {
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < tuple.size(); ++i) {
                    builder.append(tuple.get(i) == null ? "" : tuple.get(i)
                            .toString());
                    if (i != tuple.size() - 1)
                        builder.append(delimiter);
                }
                return builder.toString();
            }
        };
    }
}
