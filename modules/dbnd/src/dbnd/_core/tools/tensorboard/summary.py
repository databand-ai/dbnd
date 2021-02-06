import logging
import time

from collections import defaultdict

from dbnd._core.parameter.parameter_value import ParameterFilters
from dbnd._core.task_ctrl.task_relations import TaskRelations
from dbnd._core.utils.traversing import flatten


logger = logging.getLogger(__name__)


def tensorflow_graph(task):
    from tensorboardX.src.attr_value_pb2 import AttrValue
    from tensorboardX.src.graph_pb2 import GraphDef
    from tensorboardX.src.node_def_pb2 import NodeDef
    from tensorboardX.src.step_stats_pb2 import (
        RunMetadata,
        StepStats,
        DeviceStepStats,
        NodeExecStats,
        AllocatorMemoryUsed,
    )
    from tensorboardX.src.tensor_shape_pb2 import TensorShapeProto
    from tensorboardX.src.versions_pb2 import VersionDef

    # assert isinstance(task, Task)

    tasks = task.ctrl.task_dag.subdag_tasks()
    children = defaultdict(list)
    parents = defaultdict(list)
    for task in tasks:
        children[task.task_id] = task.descendants.get_children()
        for t_child in children[task.task_id]:
            parents[t_child.task_id].append(task)
    friendly_name = {t.task_id: t.task_af_id for t in tasks}

    def _get_name(t):
        cur_id = t.task_id
        cur_scope = [friendly_name[cur_id]]
        while parents[cur_id]:
            # right now just do simple scope, don'task take care of shared tasks
            cur_id = parents[cur_id][0].task_id
            cur_scope.append(friendly_name[cur_id])
        return "/".join(reversed(cur_scope))

    nodes = []

    # calculate scope for all tasks
    names = {}
    for task in tasks:
        names[task.task_id] = _get_name(task)

    # attrs = attrs.replace("'", ' ')  # singlequote will be escaped by tensorboard
    def _name(t, *path):
        _path = [names[t.task_id]]
        if path:
            _path.extend(list(path))
        return "/".join(_path)

    def _op_name(task):
        return _name(task, task.task_id)

    def _target_name(target):
        return _name(task.source_task, target.name or "output")

    for task in tasks:
        tr = task.ctrl.relations
        assert isinstance(tr, TaskRelations)

        task_inputs = []
        op_name = _op_name(task)

        for target in flatten(tr.task_inputs_user):
            if not target.owner:
                continue
            task_inputs.append(_target_name(target.source_task, target))

        for target in flatten(tr.task_outputs_user):
            if not target.owner:
                continue

            output_inputs = [op_name]  # current task
            if target.source_task != task:
                real_output = _target_name(target.source_task, target)
                output_inputs.append(real_output)
                task_inputs.append(real_output)
            node_info = {
                "name": _target_name(task, target),
                "op": "output",
                "task_inputs": [op_name],
                "attrs": {"path": AttrValue(s=str(target).encode(encoding="utf_8"))},
            }
            # node_info['outputsize'] = []
            nodes.append(node_info)

            attrs = {}
        for k, v in task._params.get_params_serialized(ParameterFilters.INPUTS):
            attrs[k] = AttrValue(s=v.encode(encoding="utf_8"))
        node_info = {
            "name": op_name,
            "op": task.task_name,
            "task_inputs": task_inputs,
            "attrs": attrs,
        }

        # node_info['outputsize'] = []
        nodes.append(node_info)

    # mapping = {}
    # for n in nodes:
    #     mapping[n['name']] = scope[n['name']] + '/' + \
    #                          n['op'] + '_' + n['name']
    # for n in nodes:
    #     n['name'] = mapping[n['name']]
    #     for i, s in enumerate(n['task_inputs']):
    #         n['task_inputs'][i] = mapping[s]

    node_stats = []
    graph_nodes = []
    for node in nodes:
        attrs = node.get("attrs", {})
        if "outputsize" in node.keys():
            shapeproto = TensorShapeProto(
                dim=[TensorShapeProto.Dim(size=d) for d in node["outputsize"]]
            )
            attrs["_output_shapes"] = AttrValue(
                list=AttrValue.ListValue(shape=[shapeproto])
            )

        if "exec_stats" in node:
            node_stats.append(
                NodeExecStats(
                    node_name=node["name"],
                    all_start_micros=int(time.time() * 1e7),
                    all_end_rel_micros=42,
                    memory=[
                        AllocatorMemoryUsed(
                            allocator_name="cpu",
                            total_bytes=19950829,
                            peak_bytes=19950829,
                            live_bytes=19950829,
                        )
                    ],
                )
            )

        graph_nodes.append(
            NodeDef(
                name=node["name"], op=node["op"], input=node["task_inputs"], attr=attrs
            )
        )

    stepstats = RunMetadata(
        step_stats=StepStats(
            dev_stats=[DeviceStepStats(device="/device:CPU:0", node_stats=node_stats)]
        )
    )
    return GraphDef(node=graph_nodes, versions=VersionDef(producer=22)), stepstats


def save_task_graph(task, comment="", log_dir=None):
    from tensorboardX import SummaryWriter

    try:
        with SummaryWriter(log_dir=log_dir, comment=comment) as w:
            w.file_writer.add_graph(tensorflow_graph(task))
    except Exception:
        logger.exception("Failed to save tensorflow graph")
