package zjffdu.cloud.pig.raf.extractor;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import zjffdu.cloud.pig.raf.extractor.SingleValueResultExtractor;

public class SingleValueResultExtractorTest extends TestCase {

    public void testSingleValueResultExtractor() throws IOException {
        DataBag bag = BagFactory.getInstance().newDefaultBag();
        Tuple tuple = TupleFactory.getInstance().newTuple(1);
        tuple.set(0, 12);
        bag.add(tuple);

        SingleValueResultExtractor<Integer> extractor = new SingleValueResultExtractor<Integer>();
        assertEquals((Integer) 12, extractor.extract(bag.iterator()));
    }
}
