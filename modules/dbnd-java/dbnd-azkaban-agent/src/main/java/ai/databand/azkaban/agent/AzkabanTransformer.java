package ai.databand.azkaban.agent;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.LoaderClassPath;
import javassist.bytecode.MethodInfo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.ClassFileTransformer;
import java.security.ProtectionDomain;
import java.util.Optional;

public class AzkabanTransformer implements ClassFileTransformer {

    private static final String TASK_ANNOTATION = "ai.databand.annotations.Task";

    private final boolean isVerbose;

    public AzkabanTransformer(boolean isVerbose) {
        this.isVerbose = isVerbose;
    }

    public byte[] transform(ClassLoader loader,
                            String className,
                            Class classBeingRedefined,
                            ProtectionDomain protectionDomain,
                            byte[] classfileBuffer) {
        ClassPool cp = ClassPool.getDefault();
        cp.appendClassPath(new LoaderClassPath(loader));
        Optional<CtClass> ctOpt = classInScope(cp, className, classfileBuffer);

        if (!ctOpt.isPresent()) {
            return null;
        }

        try {
            CtClass ct = ctOpt.get();
            System.out.printf("Instrumenting class %s%n", className);
            CtMethod[] declaredMethods = ct.getDeclaredMethods();

            for (CtMethod method : declaredMethods) {
                // wrap methods annotated by @task
                MethodInfo methodInfo = method.getMethodInfo();

                if (method.getName().contains("handleEvent")) {
                    if (isVerbose) {
                        System.out.printf("Instrumenting method %s%n", methodInfo.getName());
                    }

                    method.insertBefore("{ new ai.databand.azkaban.DbndEventReporter().report($1); }");
                    break;
                }
            }

            return ct.toBytecode();
        } catch (RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("frozen")) {
                return null;
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }

        return classfileBuffer;
    }

    protected Optional<CtClass> classInScope(ClassPool cp, String className, byte[] classfileBuffer) {
        if (!className.equalsIgnoreCase("azkaban/execapp/FlowRunnerManager")
            && !className.contains("azkaban/execapp/FlowRunner$JobRunnerEventListener")) {
            return Optional.empty();
        }
        try (InputStream is = new ByteArrayInputStream(classfileBuffer)) {
            CtClass ct = cp.makeClass(is);
            return Optional.of(ct);
        } catch (IOException e) {
            return Optional.empty();
        }
    }
}
