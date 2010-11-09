package com.snda.dw.pig.util;

import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

import org.apache.pig.data.Tuple;

import com.google.common.collect.Lists;

public class TuplesTest extends TestCase {

    public void testTuples() throws IOException {
        Tuple tuple = Tuples.newTuple(10);
        assertEquals(1, tuple.size());
        assertEquals(10, tuple.get(0));
        
        tuple = Tuples.newTuple(1, "1", 1.0);
        assertEquals(3, tuple.size());
        assertEquals(1, tuple.get(0));
        assertEquals("1", tuple.get(1));
        assertEquals(1.0, (Double) tuple.get(2), 1.0E-6);

        List items = Lists.newArrayList(1, "1", 1.0);
        tuple = Tuples.newTuple(items);
        assertEquals(3, tuple.size());
        assertEquals(1, tuple.get(0));
        assertEquals("1", tuple.get(1));
        assertEquals(1.0, (Double) tuple.get(2), 1.0E-06);

    }
}
