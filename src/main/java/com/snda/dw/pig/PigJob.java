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
    public PigJob setListener(PigJobListener listener);
    
    PigJobListener getListener();
    
    public Map<String, Object> getAttributes();

    void setPigServer(PigServer2 pigServer);

    /**
     * Get the output of alias and extract the result using {@link RowMapper} on it, used for without-store pig script
     * 
     * @param <E>
     * @param alias
     * @param mapper
     * @return
     * @throws IOException
     */
    <E> List<E> getOutput(String alias, RowMapper<E> mapper) throws IOException;

    /**
     * Get the output of alias and apply {@link ResultExtractor} on it, used for without-store pig script
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
     * Get the output of alias, used for without-store pig script
     * 
     * @param alias
     * @return
     * @throws IOException
     */
    Iterator<Tuple> getOutput(String alias) throws IOException;

    /**
     * Get the output from the specified path and using the specified LoadFunc, and apply {@link RowMapper} on it, used for with-store pig script.
     * 
     * @param <E>
     * @param path
     * @param loadFuncConstructor
     * @param mapper
     * @return
     * @throws IOException
     */
    <E> List<E> getOutput(Path path, String loadFuncConstructor, RowMapper<E> mapper)
            throws IOException;

    /**
     * Get the output from the specified path and using the specified LoadFunc, and apply {@link ResultExtractor} on it, used for with-store pig script.
     * 
     * @param <E>
     * @param path
     * @param loadFuncConstructor
     * @param extractor
     * @return
     * @throws IOException
     */
    <E> E getOutput(Path path, String loadFuncConstructor,
            ResultExtractor<E> extractor) throws IOException;

    /**
     * Get the output from the specified path and using the specified LoadFunc, used for with-store pig script.
     * 
     * @param path
     * @param loadFuncConstructor
     * @return
     * @throws IOException
     */
    Iterator<Tuple> getOutput(Path path, String loadFuncConstructor)
            throws IOException;

}
