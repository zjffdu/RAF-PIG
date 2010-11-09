package com.snda.dw.pig.util;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.base.Preconditions;

/**
 * Utility class for type {@link Tuple}
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * @email zjffdu@gmail.com
 * 
 */
public class Tuples {

    private static TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

    /**
     * 
     * @param tuple
     * @param index
     * @return
     * @throws IOException
     */
    public static Object getNotNullableCell(Tuple tuple, int index)
            throws IOException {
        Preconditions.checkElementIndex(index, tuple.size());
        return tuple.get(index) == null ? "" : tuple.get(index);
    }

    /**
     * Create a tuple from arbitrary number of elements.
     * 
     * @param <E>
     * @param elements
     * @return
     * @throws IOException
     */
    public static <E> Tuple newTuple(E... elements) throws IOException {
        Tuple tuple = TUPLE_FACTORY.newTuple(elements.length);
        for (int i = 0; i < elements.length; ++i) {
            tuple.set(i, elements[i]);
        }
        return tuple;
    }

    /**
     * Create tuple from a Object implements interface {@link Iterable}
     * 
     * @param elements
     * @return
     * @throws IOException
     */
    public static Tuple newTuple(Iterable elements) throws IOException {
        Tuple tuple = TUPLE_FACTORY.newTuple();
        Iterator iter = elements.iterator();
        while (iter.hasNext()) {
            tuple.append(iter.next());
        }
        return tuple;
    }
}
