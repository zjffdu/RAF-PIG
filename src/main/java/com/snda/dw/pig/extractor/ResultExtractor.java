package com.snda.dw.pig.extractor;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.data.Tuple;


/**
 * It it a basic extractor interface for result of pig script. 
 * You can extract the result to any type.
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 *
 * @param <E>
 */
public interface ResultExtractor<E> {
    
    public E extract(Iterator<Tuple> iter) throws IOException;
}
