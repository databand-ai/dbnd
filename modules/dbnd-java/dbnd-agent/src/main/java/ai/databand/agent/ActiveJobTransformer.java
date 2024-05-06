/*
 * © Copyright Databand.ai, an IBM Company 2022-2024
 */

package ai.databand.agent;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.LoaderClassPath;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.instrument.ClassFileTransformer;
import java.security.ProtectionDomain;

import ai.databand.DbndAppLog;

public class ActiveJobTransformer implements ClassFileTransformer {

    @Override
    public byte[] transform(ClassLoader loader,
                            String className,
                            Class<?> classBeingRedefined,
                            ProtectionDomain protectionDomain,
                            byte[] classfileBuffer) {
        if (!"org/apache/spark/scheduler/ActiveJob".equalsIgnoreCase(className)) {
            return classfileBuffer;
        }
        try (InputStream is = new ByteArrayInputStream(classfileBuffer)) {
            DbndAppLog.printfln(org.slf4j.event.Level.INFO, "Databand tracking of the Spark class 'ActiveJob'");
            ClassPool cp = ClassPool.getDefault();
            cp.appendClassPath(new LoaderClassPath(loader));
            CtClass ct = cp.makeClass(is);

            for (CtConstructor constructor : ct.getConstructors()) {
                if (constructor.callsSuper()) {
                    constructor.insertAfter("{ ai.databand.spark.ActiveJobTracker.track(this); }");
                }
            }
            return ct.toBytecode();
        } catch (Throwable e) {
            DbndAppLog.printfln(org.slf4j.event.Level.ERROR, "Databand tracking failed to modify the 'ActiveJob' class.");
            e.printStackTrace();
            return null;
        }
    }
}
