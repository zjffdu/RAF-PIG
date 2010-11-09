package com.snda.dw.pig.util;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.data.Tuple;


/**
 * Factory class for {@link ResultFormat}
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 *
 */
public class ResultFormatFactory {

    /**
     * Create {@link ResultFormat} using <code>recodeDelimiter</code> and <code>fieldDelimiter</code>
     * @param recordDelimiter
     * @param fieldDelimiter
     * @return
     */
    public static ResultFormat on(String recordDelimiter, String fieldDelimiter) {
        return new DefaultResultFormat(recordDelimiter,
                TupleFormatFactory.on(fieldDelimiter));
    }

    /**
     * Create {@link ResultFormat} using <code>recordDelimiter</code> and <code>tupleFormat</code>
     * @param recordDelimiter
     * @param tupleFormat
     * @return
     */
    public static ResultFormat newFormat(String recordDelimiter,
            TupleFormat tupleFormat) {
        return new DefaultResultFormat(recordDelimiter, tupleFormat);
    }

    private static class DefaultResultFormat implements ResultFormat {

        private String recordDelimiter;

        private TupleFormat tupleFormat;

        public DefaultResultFormat(String recordDelimiter,
                TupleFormat tupleFormat) {
            this.recordDelimiter = recordDelimiter;
            this.tupleFormat = tupleFormat;
        }

        @Override
        public String format(Iterator<Tuple> iter) throws IOException {
            StringBuilder builder = new StringBuilder();
            while (iter.hasNext()) {
                builder.append(tupleFormat.format(iter.next()));
                if (iter.hasNext()) {
                    builder.append(recordDelimiter);
                }
            }
            return builder.toString();
        }
    }
}
