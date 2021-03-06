package zjffdu.cloud.pig.raf.extractor;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;


/**
 * Use it when one tuple correspond to your one domain object
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 *
 * @param <E>
 */
public interface RowMapper<E> {
    
    public E map(Tuple tuple) throws IOException;
}
