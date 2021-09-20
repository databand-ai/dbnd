package ai.databand.schema;

import ai.databand.id.Uuid5;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.UUID;

public class AirflowTaskContext {

    private final String airflowInstanceUid;
    private final String airflowName;
    private final String dagId;
    private final String executionDate;
    private final String taskId;
    private final String tryNumber;

    public AirflowTaskContext(String airflowInstanceUid,
                              String airflowName,
                              String dagId,
                              String executionDate,
                              String taskId,
                              String tryNumber) {
        this.airflowInstanceUid = airflowInstanceUid;
        this.airflowName = airflowName;
        this.dagId = dagId;
        this.executionDate = executionDate;
        this.taskId = taskId;
        this.tryNumber = tryNumber;
    }

    public String getAirflowInstanceUid() {
        return airflowInstanceUid;
    }

    public String getAirflowName() {
        return airflowName;
    }

    public String getDagId() {
        return dagId;
    }

    public String getExecutionDate() {
        return executionDate;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getTryNumber() {
        return tryNumber;
    }

    @JsonIgnore
    public String getAfOperatorUid() {
        String airflowSyncRunUid = buildAirflowJobRunUid();
        return buildAirflowTaskRunUid(airflowSyncRunUid);
    }

    /**
     * def get_job_run_uid(dag_id, execution_date):
     * if airflow_instance_uid is None:
     * return uuid.uuid5(NAMESPACE_DBND_RUN, "{}:{}".format(dag_id, execution_date))
     * else:
     * return uuid.uuid5(NAMESPACE_DBND_RUN, "{}:{}:{}".format(airflow_instance_uid, dag_id, execution_date))
     */
    @JsonIgnore
    protected String buildAirflowJobRunUid() {
        if (airflowInstanceUid == null) {
            return new Uuid5(
                Uuid5.NAMESPACE_DBND_RUN,
                String.format("%s:%s", this.getDagId(), this.getExecutionDate())
            ).toString();
        } else {
            return new Uuid5(
                Uuid5.NAMESPACE_DBND_RUN,
                String.format("%s:%s:%s", this.getAirflowInstanceUid(), this.getDagId(), this.getExecutionDate())
            ).toString();
        }
    }

    /**
     * def get_task_run_uid(run_uid, dag_id, task_id):
     * return uuid.uuid5(run_uid, "{}.{}".format(dag_id, task_id))
     */
    @JsonIgnore
    protected String buildAirflowTaskRunUid(String runUid) {
        return new Uuid5(
            UUID.fromString(runUid),
            String.format("%s.%s", this.getDagId(), this.getTaskId())
        ).toString();
    }

    /**
     * def get_task_def_uid(dag_id, task_id):
     * return uuid.uuid5(NAMESPACE_DBND_TASK_DEF, "{}.{}".format(dag_id, task_id))
     */
    @JsonIgnore
    protected String buildAirflowTaskDefUid(AirflowTaskContext context) {
        return new Uuid5(
            Uuid5.NAMESPACE_DBND_TASK_DEF,
            String.format("%s.%s", context.getDagId(), context.getTaskId())
        ).toString();
    }

    /**
     * def get_task_run_attempt_uid(run_uid, dag_id, task_id, try_number):
     * return uuid.uuid5(run_uid, "{}.{}:{}".format(dag_id, task_id, try_number))
     */
    @JsonIgnore
    protected String buildAirflowTaskRunAttemptUid(String runUid, AirflowTaskContext context) {
        return new Uuid5(
            UUID.fromString(runUid),
            String.format("%s.%s:%s", context.getDagId(), context.getTaskId(), context.getTryNumber())
        ).toString();
    }

    /**
     * def get_job_uid(dag_id):
     * if airflow_server_info_uid:
     * return uuid.uuid5(NAMESPACE_DBND_JOB, "{}:{}".format(airflow_server_info_uid, dag_id))
     * else:
     * return uuid.uuid5(NAMESPACE_DBND_JOB, dag_id)
     */
    @JsonIgnore
    protected String buildAirflowJobUid(AirflowTaskContext context) {
        if (context.getAirflowInstanceUid() == null) {
            return new Uuid5(Uuid5.NAMESPACE_DBND_JOB, context.getDagId()).toString();
        } else {
            return new Uuid5(
                Uuid5.NAMESPACE_DBND_JOB,
                String.format("%s:%s", context.getAirflowInstanceUid(), context.getDagId())
            ).toString();
        }
    }

    @JsonIgnore
    public String jobName() {
        return String.format("%s.%s", dagId, taskId);
    }
}
