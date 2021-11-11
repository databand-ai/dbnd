package ai.databand.agent;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.LoaderClassPath;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.instrument.ClassFileTransformer;
import java.security.ProtectionDomain;

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
            System.out.printf("Instrumenting class %s%n", className);
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
            System.err.println("Spark ActiveJob class instrumentation failed. I/O tracking won't be available.");
            e.printStackTrace();
            return null;
        }
    }
}
