package ai.databand.agent;

import ai.databand.config.DbndConfig;

import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.Instrumentation;
import java.util.Properties;

public class DbndAgent {

    public static void premain(String agentArgs, Instrumentation inst) {
        Properties props = new Properties();
        try (InputStream input = DbndAgent.class.getClassLoader().getResourceAsStream("application.properties")) {
            props.load(input);
        } catch (IOException e) {
            // shouldn't occur
            e.printStackTrace();
        }
        System.out.printf("Starting Databand v%s!%n", props.getProperty("version"));
        // this is workaround for spark-submit case
        // for some reason CallSite is not loaded during instrumentation phase, so we have to load it before
        try {
            Class.forName("java.lang.invoke.CallSite");
        } catch (Throwable e) {
            e.printStackTrace();
        }
        DbndConfig config = new DbndConfig();
        inst.addTransformer(new DbndTrackingTransformer(config));
        if (config.sparkIoTrackingEnabled()) {
            inst.addTransformer(new ActiveJobTransformer());
        }
    }

}
