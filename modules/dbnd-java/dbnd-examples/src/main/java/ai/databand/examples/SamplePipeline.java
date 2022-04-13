package ai.databand.examples;

import ai.databand.annotations.Task;

import java.security.SecureRandom;

public class SamplePipeline {

    private final int firstValue;
    private final String secondValue;

    public SamplePipeline(int firstValue, String secondValue) {
        this.firstValue = firstValue;
        this.secondValue = secondValue;
    }

    @Task("jvm_pipeline")
    public void doStuff() {
        String firstResult = stepOne(firstValue, secondValue);
        int secondResult = stepTwo(firstResult);
        stepTwo(String.valueOf(secondResult));
        processData(secondResult);
        nullTask(null);
        try {
            badTask(secondResult);
        } catch (Exception e) {
            // do nothing
        }
    }

    @Task
    public void processData(int data) {
        // do nothing
    }

    @Task
    public Object nullTask(Object value) {
        return null;
    }

    @Task
    public String stepOne(int first, String second) {
        return first + second;
    }

    @Task
    public int stepTwo(String third) {
        int value = firstNestedTask(third.hashCode());
        int value2 = secondNestedTask(third.hashCode() + new SecureRandom().nextInt(10000));
        return third.hashCode() + value + value2;
    }

    @Task
    void badTask(double input) {
        if (input > 1) {
            throw new RuntimeException("Error!");
        }
    }

    @Task
    int firstNestedTask(int input) {
        return input * 2;
    }

    @Task
    int secondNestedTask(int input) {
        thirdNestedTask(String.valueOf(input + 1000));
        return input * 2;
    }

    @Task
    String thirdNestedTask(String input) {
        return input + input.hashCode();
    }
}
