package com.snda.dw.pig;

import java.io.IOException;

import com.snda.dw.pig.PigConfiguration.Options;


/**
 * Factory class for {@link PigServer2}
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
 * @email zjffdu@gmail.com 
 *
 */
public class PigServer2Factory {

    private PigConfiguration conf;

    public PigServer2Factory(PigConfiguration conf) {
        this.conf = conf;
    }

    public PigServer2 getPigServer() throws IOException {
        PigServer2 pig = new PigServer2(conf.get(Options.ExecType));
        pig.getPigContext().initializeImportList(conf.get(Options.UDFPackage));
        pig.registerJar(conf.get(Options.UDFJar));
        return pig;
    }
}
