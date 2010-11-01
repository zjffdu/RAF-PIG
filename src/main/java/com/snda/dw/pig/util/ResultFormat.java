package com.snda.dw.pig.util;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * Format class for result of PigScript. Users sometimes complain that the Pig's
 * internal plain text representation of {@link Tuple} and {@link DataBag},
 * especially there's nested {@link Tuple} and {@link DataBag}. It is hard to
 * parse it for users outside pig world, especially when handle result using other
 * programming languages. <br/>
 * e.g. You have a result as following
 * 
 * <pre>
 * (John,(1,2,3))
 * (Lucy,(2,3,4)}
 * </pre>
 * 
 * You can use this interface to convert it into the following format which you
 * can handle it easily.
 * 
 * <pre>
 * John,1,2,3
 * Lucy,2,3,4
 * </pre>
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * @email zjffdu@gmail.com 
 *
 */
public interface ResultFormat {

    public String format(Iterator<Tuple> iter) throws IOException;
}
