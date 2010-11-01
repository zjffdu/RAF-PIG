package com.snda.dw.pig.util;

import java.io.IOException;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * Format class for {@link Tuple} which convert a tuple to string.
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * @email zjffdu@gmail.com 
 * 
 */
public interface TupleFormat {

    public String format(Tuple tuple) throws IOException;

}
