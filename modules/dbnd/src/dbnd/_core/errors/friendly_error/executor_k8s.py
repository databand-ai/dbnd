from dbnd._core.errors import DatabandConfigError


def local_engine_not_accept_remote_jobs(env, to_cluster):
    return DatabandConfigError(
        "cluster  '{to_cluster}' can't accept remote jobs".format(
            to_cluster=to_cluster
        ),
        help_msg="Check your execution configuration at {}".format(env),
    )


def kubernetes_with_non_compatible_engine(task_engine):
    return DatabandConfigError(
        "Config env.task_executor_type expected to be airflow_kubernetes for {}".format(
            task_engine.task_name
        )
    )


class DatabandKubernetesError(DatabandConfigError):
    pass


class KubernetesImageNotFoundError(DatabandKubernetesError):
    pass


class KubernetesRunningPodConditionFailure(DatabandKubernetesError):
    pass


class KubernetesPodConfigFailure(DatabandKubernetesError):
    pass


def kubernetes_image_not_found(image_name, message, long_msg):
    return KubernetesImageNotFoundError(
        "Failed to start Kubernetes pod because the configured image (%s) could not be pulled by Kubernetes: %s"
        % (image_name, message),
        help_msg="Make sure you have built and pushed your image. "
        "If the image is in a private repository make sure you "
        "configured image pull secrets for it in the Kubernetes cluster "
        "and configured image_pull_secrets in the Kubernetes engine config. Details %s"
        % long_msg,
    )


def kubernetes_running_pod_fails_on_condition(condition, pod_name):
    return KubernetesRunningPodConditionFailure(
        "Pod %s is failing at Running phase: %s" % (pod_name, condition),
        help_msg="The pod is failing. Main reason for that is preemptible nodes, "
        "you can disable this check at `kubernetes.check_running_pod_errors=False`. "
        "See pod log for more details",
    )


def kubernetes_pod_config_error(kub_message):
    help_msg = ""
    if "databand-secrets" in kub_message:
        help_msg = (
            "by default Databand assumes a secret exists in Kubernetes with the name databand-secrets "
            "containing the db connection string and airflow fernet key. "
            "Either create the secret (see the kubernetes deployment script) or override the secrets "
            "property in your Kubernetes engine config section"
        )
    elif kub_message.startswith("secret"):
        help_msg = "Check that the secret exsits in the cluster or remove it from the engine config"

    return KubernetesPodConfigFailure(
        "Failed to start Kubernetes pod because of a configuration error: %s"
        % kub_message,
        help_msg=help_msg,
    )


def kubernetes_pod_unschedulable(kub_message, extra_help=None):
    help_msg = ""
    if "taints" in kub_message:
        help_msg = "Either remove taints from at least one of the Kubernetes nodes or add tolerations to the Kubernetes engine config."

    if extra_help:
        help_msg = help_msg + "\n" + extra_help

    return DatabandConfigError(
        "Failed to start Kubernetes pod because it couldn't be scheduled. Reason: %s"
        % kub_message,
        help_msg=help_msg,
    )


def no_tag_on_no_build():
    return DatabandConfigError(
        "You are running with 'docker_build=False', however, 'container_tag' is not defined",
        help_msg="Please configure 'container_ta'g or change 'docker_build' to 'True'.",
    )


def failed_to_connect_to_cluster(in_cluster_value, exc):
    return DatabandConfigError(
        "Could not connect to kubernetes cluster! Exception: %s" % (exc,),
        help_msg="in-cluster is set to '%s'. Are you running %s cluster?"
        % (in_cluster_value, "inside" if in_cluster_value else "outside"),
    )
