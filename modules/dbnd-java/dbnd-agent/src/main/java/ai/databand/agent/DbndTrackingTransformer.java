/*
 * © Copyright Databand.ai, an IBM Company 2022-2024
 */

package ai.databand.agent;

import ai.databand.DbndAppLog;
import ai.databand.config.DbndAgentConfig;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.LoaderClassPath;
import javassist.NotFoundException;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.DuplicateMemberException;
import javassist.bytecode.MethodInfo;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.management.ManagementFactory;
import java.security.ProtectionDomain;
import java.util.LinkedList;
import java.util.List;

public class DbndTrackingTransformer implements ClassFileTransformer {

    private static final String TASK_ANNOTATION = "ai.databand.annotations.Task";

    private final DbndAgentConfig config;

    public DbndTrackingTransformer(DbndAgentConfig config) {
        this.config = config;
    }

    public byte[] transform(ClassLoader loader,
                            String className,
                            Class classBeingRedefined,
                            ProtectionDomain protectionDomain,
                            byte[] classfileBuffer) {
        ClassPool cp = ClassPool.getDefault();
        cp.appendClassPath(new LoaderClassPath(loader));

        try(InputStream is = new ByteArrayInputStream(classfileBuffer)) {
            CtClass ct = cp.makeClass(is);
            String jvmName = ManagementFactory.getRuntimeMXBean().getName();

            List<CtMethod> annotatedMethods = getAnnotatedMethods(cp, ct, className, classfileBuffer);
            if (annotatedMethods.isEmpty()) {
                //DbndAppLog.printfvln("No valid @Task annotated methods detected for the class '%s' in JVM '%s' - skipped databand tracking for this class", className, jvmName);

                return null;
            }

            // add $dbnd variable
            try {
                ct.addField(CtField.make("static ai.databand.DbndWrapper $dbnd = ai.databand.DbndWrapper.instance();", ct));
            } catch (DuplicateMemberException e) {
                // do nothing
            }

            for (CtMethod method : annotatedMethods) {
                // wrap methods annotated by @Task
                MethodInfo methodInfo = method.getMethodInfo();
                CtClass tr = cp.get("java.lang.Throwable");

                DbndAppLog.printfvln("Databand tracking of @Task annotated method '%s.%s()'", className, methodInfo.getName());

                method.insertBefore("{ $dbnd.beforeTask(\"" + ct.getName() + "\", \"" + method.getLongName() + "\", $args); }");
                method.insertAfter("{ $dbnd.afterTask(\"" + method.getLongName() + "\", (Object) ($w) $_); }");
                method.addCatch("{ $dbnd.errorTask(\"" + method.getLongName() + "\", $e); throw $e; }", tr);
            }

            DbndAppLog.printfln(org.slf4j.event.Level.INFO, "Databand has succesfully detected and has started tracking of %d @Task annotated methods out of %d total methods declared directly inside the class '%s'", annotatedMethods.size(), ct.getDeclaredMethods().length, className);

            return ct.toBytecode();
        } catch (RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("frozen")) {
                return null;
            }
        } catch (Throwable e) {
            DbndAppLog.printfln(org.slf4j.event.Level.ERROR, "Databand failed to add runtime tracking to class %s", className);
            e.printStackTrace();
            return null;
        }

        return classfileBuffer;
    }

    protected List<CtMethod> getAnnotatedMethods(ClassPool cp, CtClass ct, String className, byte[] classfileBuffer) {
        List<CtMethod> annotatedMethods = new LinkedList<CtMethod>();
        CtMethod[] declaredMethods = ct.getDeclaredMethods();
        for (CtMethod method : declaredMethods) {
            MethodInfo methodInfo = method.getMethodInfo();
            AnnotationsAttribute attInfo = (AnnotationsAttribute) methodInfo.getAttribute(AnnotationsAttribute.visibleTag);
            if (attInfo == null) {
                continue;
            }
            if (attInfo.getAnnotation(TASK_ANNOTATION) != null) {
                // check if scala object
                if (!isScalaObject(cp, className)) {
                    return new LinkedList<CtMethod>();
                }
                annotatedMethods.add(method);
            }
        }

        return annotatedMethods;
    }

    protected boolean isScalaObject(ClassPool cp, String className) {
        if (className.contains("$")) {
            // this is (probably) scala class
            return true;
        }
        // this can be scala object. Let's check if it has class with '$'
        try {
            cp.get(className + '$');
            // oops! there is actual scala class in classpath
            return false;
        } catch (NotFoundException e) {
            return true;
        }
    }
}
