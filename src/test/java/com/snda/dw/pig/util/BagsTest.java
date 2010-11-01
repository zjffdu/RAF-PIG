package com.snda.dw.pig.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class BagsTest extends TestCase {

    public void testBags() throws IOException {
        DataBag bag = Bags.newBag(3, 1, "1", 1.0, 2, "2", 2.0);
        assertEquals(2, bag.size());
        Iterator<Tuple> iter = bag.iterator();
        Tuple tuple = iter.next();
        assertEquals(3, tuple.size());
        assertEquals(1, tuple.get(0));
        assertEquals("1", tuple.get(1));
        assertEquals(1.0, (Double) tuple.get(2), 1.0E-6);

        tuple = iter.next();
        assertEquals(3, tuple.size());
        assertEquals(2, tuple.get(0));
        assertEquals("2", tuple.get(1));
        assertEquals(2.0, (Double) tuple.get(2), 1.0E-6);

        // tree map is sorted, so the first tuple is (zjf,10), the second is (zju,20.0)
        Map map = new TreeMap();
        map.put("zjf", 10);
        map.put("zju", 20.0);
        bag = Bags.newBag(map);
        assertEquals(2, bag.size());
        iter = bag.iterator();
        tuple = iter.next();
        assertEquals(2, tuple.size());
        assertEquals("zjf", tuple.get(0));
        assertEquals(10, tuple.get(1));
        tuple=iter.next();
        assertEquals(2, tuple.size());
        assertEquals("zju", tuple.get(0));
        assertEquals(20.0, (Double)tuple.get(1),1.0E-6);
    }
}
