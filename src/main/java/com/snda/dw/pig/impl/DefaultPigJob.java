package com.snda.dw.pig.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.apache.pig.tools.parameters.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.snda.dw.pig.PigJob;
import com.snda.dw.pig.PigServer2;
import com.snda.dw.pig.extractor.ResultExtractor;
import com.snda.dw.pig.extractor.RowMapper;
import com.snda.dw.pig.listener.PigJobListener;
import com.snda.dw.pig.listener.PigJobListenerAdapter;

/**
 * Default implementation of interface {@link PigJob}
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * @email zjffdu@gmail.com
 * 
 */
public class DefaultPigJob implements PigJob {

    private static Logger LOGGER = LoggerFactory.getLogger(DefaultPigJob.class);

    // the default jobName is the pig file name
    private String jobName;

    private InputStream scriptInputStream;

    private String rawScript;

    // the default priority is 'NORMAL'
    private String priority = "NORMAL";

    private Map<String, String> parameters = new HashMap<String, String>();

    // the default listener is doing nothing
    private PigJobListener jobListener = new PigJobListenerAdapter();

    // just for extension
    private Map<String, Object> attributes = new HashMap<String, Object>();

    private PigServer2 pigServer;

    public DefaultPigJob(InputStream in) throws IOException {
        setScriptSource(in);
    }

    @Override
    public PigJob setJobName(String jobName) {
        this.jobName = jobName;
        return this;
    }

    @Override
    public PigJob setPriority(String priority) {
        this.priority = priority;
        return this;
    }

    @Override
    public String getJobName() {
        return this.jobName;
    }

    @Override
    public String getPriority() {
        return this.priority;
    }

    @Override
    public PigJob setListener(PigJobListener listener) {
        this.jobListener = listener;
        return this;
    }

    @Override
    public PigJobListener getListener() {
        return this.jobListener;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return this.attributes;
    }

    @Override
    public PigJob setScriptSource(InputStream in) throws IOException {
        try {
            this.scriptInputStream = in;
            this.rawScript = new String(
                    ByteStreams.toByteArray(this.scriptInputStream), "UTF-8");
            return this;
        } catch (UnsupportedCharsetException e) {
            throw new IOException(e);
        }
    }

    @Override
    public String getScript() throws IOException {
        // transform the map type to list type which can been accepted by
        // ParameterSubstitutionPreprocessor
        List<String> paramList = new ArrayList<String>();
        if (parameters != null) {
            for (Map.Entry<String, String> entry : parameters.entrySet()) {
                paramList.add(entry.getKey() + "=" + entry.getValue());
            }
        }

        // do parameter substitution
        try {
            ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor(
                    50);
            StringWriter writer = new StringWriter();
            psp.genSubstitutedFile(new BufferedReader(new StringReader(
                    rawScript)), writer,
                    paramList.size() > 0 ? paramList.toArray(new String[0])
                            : null, null);
            return writer.toString();
        } catch (ParseException e) {
            throw new IOException(e);
        }

    }

    @Override
    public PigJob setParameter(String key, String value) {
        this.parameters.put(key, value);
        return this;
    }

    @Override
    public PigJob setParameters(Map<String, String> params) {
        this.parameters = params;
        return this;
    }

    @Override
    public Map<String, String> getParameters() {
        return this.parameters;
    }

    @Override
    public String getParameter(String key) {
        String value = this.parameters.get(key);
        if (value == null || value.trim().length() == 0) {
            LOGGER.warn("Parameter '{}' is null or empty", key);
        }
        return value;
    }

    @Override
    public void setPigServer(PigServer2 pigServer) {
        this.pigServer = pigServer;
    }

    @Override
    public <E> List<E> getOutput(String alias, RowMapper<E> mapper)
            throws IOException {
        List<E> list = new ArrayList<E>();
        Iterator<Tuple> iter = getOutput(alias);
        while (iter.hasNext()) {
            list.add(mapper.map(iter.next()));
        }
        return list;
    }

    @Override
    public <E> E getOutput(String alias, ResultExtractor<E> extractor)
            throws IOException {
        Iterator<Tuple> iter = getOutput(alias);
        return extractor.extract(iter);
    }

    @Override
    public Iterator<Tuple> getOutput(String alias) throws IOException {
        return this.pigServer.openIterator(alias);
    }

    @Override
    public <E> E getOutput(Path path, String loadFuncClass,
            ResultExtractor<E> extractor) throws IOException {
        Iterator<Tuple> iter = getOutput(path, loadFuncClass);
        return extractor.extract(iter);
    }

    @Override
    public <E> List<E> getOutput(Path path, String loadFuncClass,
            RowMapper<E> mapper) throws IOException {
        List<E> list = new ArrayList<E>();
        Iterator<Tuple> iter = getOutput(path, loadFuncClass);
        while (iter.hasNext()) {
            list.add(mapper.map(iter.next()));
        }
        return list;
    }

    @Override
    public Iterator<Tuple> getOutput(Path path, String loadFuncSpec)
            throws IOException {

        final LoadFunc p;
        FuncSpec funcSpec = new FuncSpec(loadFuncSpec);
        FileSpec fileSpec = new FileSpec(path.toString(), funcSpec);
        try {
            LoadFunc originalLoadFunc = (LoadFunc) PigContext
                    .instantiateFuncFromSpec(funcSpec);

            p = (LoadFunc) new ReadToEndLoader(originalLoadFunc,
                    ConfigurationUtil.toConfiguration(pigServer.getPigContext()
                            .getProperties()), fileSpec.getFileName(), 0);
        } catch (Exception e) {
            int errCode = 2088;
            String msg = "Unable to get results for: " + fileSpec;
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }

        return new Iterator<Tuple>() {
            Tuple t;
            boolean atEnd;

            public boolean hasNext() {
                if (atEnd)
                    return false;
                try {
                    if (t == null)
                        t = p.getNext();
                    if (t == null)
                        atEnd = true;
                } catch (Exception e) {
                    LOGGER.error(Throwables.getStackTraceAsString(e));
                    t = null;
                    atEnd = true;
                    throw new Error(e);
                }
                return !atEnd;
            }

            public Tuple next() {
                Tuple next = t;
                if (next != null) {
                    t = null;
                    return next;
                }
                try {
                    next = p.getNext();
                } catch (Exception e) {
                    LOGGER.error(Throwables.getStackTraceAsString(e));
                }
                if (next == null)
                    atEnd = true;
                return next;
            }

            public void remove() {
                throw new RuntimeException("Removal not supported");
            }

        };
    }

}
