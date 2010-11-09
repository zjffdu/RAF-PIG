package com.snda.dw.pig.util;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.base.Preconditions;

/**
 * Utility class for type {@link DataBag}
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * 
 */
public class Bags {

    private static BagFactory BAG_FACTORY = BagFactory.getInstance();

    private static TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

    /**
     * Create {@link DataBag} from arbitrary number of elements, each element is
     * a cell in tuple, and <code>tupleSize</code> specify the size of tuple.
     * 
     * @param <E>
     * @param tupleSize
     * @param elements  
     * @return
     * @throws IOException
     */
    public static <E> DataBag newBag(int tupleSize, E... elements)
            throws IOException {
        Preconditions.checkArgument(tupleSize > 0,
                "tupleSize must be creater than 0");
        Preconditions.checkArgument(elements.length % tupleSize == 0,
                "elements size is not integer multiples of tupleSize");

        DataBag bag = BAG_FACTORY.newDefaultBag();
        int start = 0;
        Tuple tuple = TUPLE_FACTORY.newTuple(tupleSize);
        for (int i = 0; i < elements.length; ++i) {
            tuple.set(start++, elements[i]);
            if (start == tupleSize) {
                start = 0;
                bag.add(tuple);
                tuple = TUPLE_FACTORY.newTuple(tupleSize);
            }
        }
        return bag;
    }

    /**
     * Create {@link DataBag} from arbitrary number of element, and each element
     * in must implement interface {@link Iterable} which correspond to one
     * tuple.
     * 
     * @param <E>
     * @param elements
     * @return
     * @throws IOException
     */
    public static <E extends Iterable> DataBag newBag(E... elements)
            throws IOException {
        DataBag bag = BAG_FACTORY.newDefaultBag();
        for (E element : elements) {
            bag.add(Tuples.newTuple(element));
        }
        return bag;
    }

    /**
     * Create {@link DataBag} from map, each entry in map correspond to one
     * {@link Tuple}, the key is first element while the value is the second
     * element in tuple.
     * 
     * @param map
     * @return
     * @throws IOException
     */
    public static DataBag newBag(Map<Object, Object> map) throws IOException {
        DataBag bag = BAG_FACTORY.newDefaultBag();
        for (Map.Entry entry : map.entrySet()) {
            Tuple tuple = Tuples.newTuple(entry.getKey(), entry.getValue());
            bag.add(tuple);
        }
        return bag;
    }

}
