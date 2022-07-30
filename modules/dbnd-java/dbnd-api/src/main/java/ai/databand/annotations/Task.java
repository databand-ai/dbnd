/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation marks method as a logical part of pipeline. Every metric captured
 * during method execution will be reported to Databand. Nested methods, not annotated as @Task
 * will treated as the same method.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Task {

    String value() default "";

}
