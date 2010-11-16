package zjffdu.cloud.pig.raf;

import java.io.IOException;

import zjffdu.cloud.pig.raf.PigConfiguration.Options;



/**
 * Factory class for {@link PigServer2}
 * 
 * @author <a href="http://zjffdu.blogspot.com/">Jeff Zhang</a>
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
        String[] jars=conf.get(Options.UDFJar).split(":");
        for (String jar:jars){
            pig.registerJar(jar);
        }
        return pig;
    }
}
