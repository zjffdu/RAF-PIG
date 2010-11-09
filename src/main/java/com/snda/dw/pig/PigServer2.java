package com.snda.dw.pig;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

/**
 * This is our own PigServer which add more features to the {@link PigServer}
 * Pig-team provides. This allows us add new features without dependency on pig
 * official release.
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * 
 */
public class PigServer2 extends PigServer {

    private static Logger LOGGER = LoggerFactory.getLogger(PigServer2.class);

    public PigServer2(ExecType execType) throws ExecException {
        super(execType);
    }

    public PigServer2(String execTypeString) throws ExecException, IOException {
        super(execTypeString);
    }

    /**
     * Register a pig script from InputStream source which is more general and
     * extensible the pig script can be from local file, then you can use
     * FileInputStream. or pig script can be in memory which you build it
     * dynamically, the you can use ByteArrayInputStream even pig script can be
     * in remote machine, which you get wrap it as SocketInputStream
     * 
     * @param in
     * @throws IOException
     */
    public void registerScript(InputStream in) throws IOException {
        registerScript(in, null, null);
    }

    /**
     * Register a pig script from InputStream source which is more general and
     * extensible the pig script can be from local file, then you can use
     * FileInputStream. or pig script can be in memory which you build it
     * dynamically, the you can use ByteArrayInputStream even pig script can be
     * in remote machine, which you get wrap it as SocketInputStream. The
     * parameters in the pig script will be substituted with the values in
     * params
     * 
     * @param in
     * @param params
     *            the key is the parameter name, and the value is the parameter
     *            value
     * @throws IOException
     */
    public void registerScript(InputStream in, Map<String, String> params)
            throws IOException {
        registerScript(in, params, null);
    }

    /**
     * Register a pig script from InputStream source which is more general and
     * extensible the pig script can be from local file, then you can use
     * FileInputStream. or pig script can be in memory which you build it
     * dynamically, the you can use ByteArrayInputStream even pig script can be
     * in remote machine, which you get wrap it as SocketInputStream The
     * parameters in the pig script will be substituted with the values in the
     * parameter files
     * 
     * @param in
     * @param paramsFiles
     *            files which have the parameter setting
     * @throws IOException
     */
    public void registerScript(InputStream in, List<String> paramsFiles)
            throws IOException {
        registerScript(in, null, paramsFiles);
    }

    /**
     * Register a pig script from InputStream source which is more general and
     * extensible the pig script can be from local file, then you can use
     * FileInputStream. or pig script can be in memory which you build it
     * dynamically, the you can use ByteArrayInputStream even pig script can be
     * in remote machine, which you get wrap it as SocketInputStream. The
     * parameters in the pig script will be substituted with the values in the
     * map and the parameter files. The values in params Map will override the
     * value in parameter file if they have the same parameter
     * 
     * @param in
     * @param params
     *            the key is the parameter name, and the value is the parameter
     *            value
     * @param paramsFiles
     *            files which have the parameter setting
     * @throws IOException
     */
    public void registerScript(InputStream in, Map<String, String> params,
            List<String> paramsFiles) throws IOException {
        try {
            // transform the map type to list type which can been accepted by
            // ParameterSubstitutionPreprocessor
            List<String> paramList = new ArrayList<String>();
            if (params != null) {
                for (Map.Entry<String, String> entry : params.entrySet()) {
                    paramList.add(entry.getKey() + "=" + entry.getValue());
                }
            }

            // do parameter substitution
            ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor(
                    50);
            StringWriter writer = new StringWriter();
            psp.genSubstitutedFile(
                    new BufferedReader(new InputStreamReader(in)), writer,
                    paramList.size() > 0 ? paramList.toArray(new String[0])
                            : null,
                    paramsFiles != null ? paramsFiles.toArray(new String[0])
                            : null);

            GruntParser grunt = new GruntParser(new StringReader(
                    writer.toString()));
            grunt.setInteractive(false);
            grunt.setParams(this);
            grunt.parseStopOnError(true);
        } catch (FileNotFoundException e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
            throw new IOException(e.getCause());
        } catch (org.apache.pig.tools.pigscript.parser.ParseException e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
            throw new IOException(e.getCause());
        } catch (org.apache.pig.tools.parameters.ParseException e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
            throw new IOException(e.getCause());
        }
    }

}
