package ai.databand.examples;

import ai.databand.annotations.Task;

public class InlineMainPipeline {

    private final String arg;

    public InlineMainPipeline(String arg) {
        this.arg = arg;
    }

    @Task
    public static void main(String args[]) {
        System.out.println("Running pipeline");
        InlineMainPipeline inline = new InlineMainPipeline("test");
        inline.firstTask();
        inline.secondTask();
    }

    @Task
    public void firstTask() {
        System.out.println("Running first task");
    }

    @Task
    public void secondTask() {
        System.out.println("Running second task");
    }

}
