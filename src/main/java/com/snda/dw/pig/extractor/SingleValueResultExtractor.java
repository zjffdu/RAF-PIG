package com.snda.dw.pig.extractor;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;


/**
 * Use it when your pig script has only one value in output, e.g. total number of visitors of your web site.
 * <pre>
 * a = load 'input_location' as (uuid,....);
 * b = foreach a generate uuid;
 * c = group b all;
 * d = foreach c generate COUNT(b.uuid);
 * store d into 'output_location';
 * </pre>
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 *
 * @param <E>
 */
public class SingleValueResultExtractor<E> implements ResultExtractor<E>{

    @Override
    public E extract(Iterator<Tuple> iter) throws IOException {
        return (E) (iter.next().get(0));
    }

}
