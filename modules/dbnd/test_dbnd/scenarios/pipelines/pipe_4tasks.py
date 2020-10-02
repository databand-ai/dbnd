from dbnd import PipelineTask, PythonTask, data, namespace, output, parameter


namespace("scenario_4_tasks", scope=__name__)


class _F4Task(PythonTask):
    t_param = parameter[str]
    o_output = output

    def run(self):
        self.o_output.write("done %s\n" % self.t_param)


class A1_F4Task(_F4Task):
    t_param = parameter.value(default="A1")


class A2_F4Task(_F4Task):
    t_param = parameter.value(default="A2")


class B_F4Task(PythonTask):
    t_param = parameter.value(default="B")
    a1_input = data

    o_output = output

    def run(self):
        self.o_output.write("done %s\n" % self.t_param)


class C_F4Task(PythonTask):
    t_param = parameter.value("C")
    b_input = parameter.data
    a2_input = parameter.data

    o_output = output

    def run(self):
        self.o_output.write("done %s\n" % self.t_param)


class MainPipeline(PipelineTask):
    c_output = output

    def band(self):
        a1 = A1_F4Task()
        a2 = A2_F4Task()
        b = B_F4Task(a1_input=a1.o_output)
        self.c_output = C_F4Task(a2_input=a2.o_output, b_input=b.o_output)
