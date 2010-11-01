package com.snda.dw.pig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.pig.data.Tuple;

import com.snda.dw.pig.extractor.ResultExtractor;
import com.snda.dw.pig.extractor.RowMapper;
import com.snda.dw.pig.listener.PigJobListener;

/**
 * Basic interface for a pig script. Every pig-related information can been set
 * using this interface.
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * @email zjffdu@gmail.com 
 * 
 */
public interface PigJob {

    /**
     * 
     * @param jobName
     * @return
     */
    public PigJob setJobName(String jobName);

    public String getJobName();

    /**
     * 
     * @param priority
     * @return
     */
    public PigJob setPriority(String priority);

    /**
     * 
     * @param key
     * @param value
     * @return
     */
    public PigJob setParameter(String key, String value);

    /**
     * 
     * @param params
     * @return
     */
    public PigJob setParameters(Map<String, String> params);

    public Map<String, String> getParameters();

    public String getParameter(String key);

    public String getPriority();

    /**
     * 
     * @param in
     * @return
     * @throws IOException
     */
    public PigJob setScriptSource(InputStream in) throws IOException;

    /**
     * Get the string representation of pig script after the parameter substitution.
     * 
     * @return
     * @throws IOException
     */
    public String getScript() throws IOException;

    /**
     * 
     * @param listener
     * @return
     */
    public PigJob addListener(PigJobListener listener);
    
    List<PigJobListener> getListeners();

    public Map<String, Object> getAttributes();

    void setPigServer(PigServer2 pigServer);

    /**
     * 
     * @param <E>
     * @param alias
     * @param mapper
     * @return
     * @throws IOException
     */
    <E> List<E> getOutput(String alias, RowMapper<E> mapper) throws IOException;

    /**
     * 
     * @param <E>
     * @param alias
     * @param extractor
     * @return
     * @throws IOException
     */
    <E> E getOutput(String alias, ResultExtractor<E> extractor)
            throws IOException;

    /**
     * 
     * @param alias
     * @return
     * @throws IOException
     */
    Iterator<Tuple> getOutput(String alias) throws IOException;

    <E> List<E> getOutput(Path path, String loadFuncClass, RowMapper<E> mapper)
            throws IOException;

    <E> E getOutput(Path path, String loadFuncClass,
            ResultExtractor<E> extractor) throws IOException;

    Iterator<Tuple> getOutput(Path path, String loadFuncClass)
            throws IOException;

}
