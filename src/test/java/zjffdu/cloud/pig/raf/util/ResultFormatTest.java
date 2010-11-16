package zjffdu.cloud.pig.raf.util;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.pig.data.DataBag;

import zjffdu.cloud.pig.raf.util.Bags;
import zjffdu.cloud.pig.raf.util.ResultFormat;
import zjffdu.cloud.pig.raf.util.ResultFormatFactory;

public class ResultFormatTest extends TestCase {

    public void testResultFormat() throws IOException {
        DataBag bag = Bags.newBag(2, 1, "1", 2, "2");
        ResultFormat format = ResultFormatFactory.on("\t", ":");
        assertEquals("1:1\t2:2", format.format(bag.iterator()));
    }
}
