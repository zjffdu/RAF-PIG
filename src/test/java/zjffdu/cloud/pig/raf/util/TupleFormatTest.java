package zjffdu.cloud.pig.raf.util;

import java.io.IOException;

import junit.framework.TestCase;


import org.apache.pig.data.Tuple;

import zjffdu.cloud.pig.raf.util.TupleFormat;
import zjffdu.cloud.pig.raf.util.TupleFormatFactory;
import zjffdu.cloud.pig.raf.util.Tuples;

public class TupleFormatTest extends TestCase {

    
    public void testTupleFormat() throws IOException {
        TupleFormat format = TupleFormatFactory.on(":");
        Tuple tuple = Tuples.newTuple(1, "1", 1.0);
        assertEquals("1:1:1.0", format.format(tuple));

        tuple=Tuples.newTuple("1",null);
        assertEquals("1:", format.format(tuple));

        TupleFormat dummyFormat = new TupleFormat() {
            @Override
            public String format(Tuple tuple) throws IOException {
                return "Dummy";
            }
        };

        assertEquals("Dummy", dummyFormat.format(tuple));
    }
}
