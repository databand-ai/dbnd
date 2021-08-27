# ORIGIN: https://github.com/databricks/mlflow
import os
import shutil

from dbnd import task
from dbnd._vendor.tmpdir import TempDir
from dbnd_examples.orchestration.examples.wine_quality.serving.utils import run_commands


REPOSITORY_NAME = "databand-serving"

"""
This task create a simple docker image with a Flask application which serves skilearn model. The serving is
exposed at docker port 8080. It supports two commands:
    ping - return OK if server is up and running
    invocation - gets pandas dataframe as json, run model on each row and return result as dataframe

Example:
    curl -X POST -H "Content-Type:application/json; format=pandas-split" --data '{"columns":["alcohol", "chlorides", "citric acid", "density", "fixed acidity", "free sulfur dioxide", "pH", "residual sugar", "sulphates", "total sulfur dioxide", "volatile acidity"],"data":[[12.8, 0.029, 0.48, 0.98, 6.2, 29, 3.33, 1.2, 0.39, 75, 0.66]]}' http://127.0.0.1:1234/invocations
    returns [6.228663402612696]

    curl -v localhost:1234/ping
    returns HTTP/1.0 200 OK

Run docker container
    The image is stored locally repository name is databand-serving with a train task signature as a tag
    To start a container and listen on local port 1234 run:
    docker run -p 1234:8080 databand_serving

"""


@task
def package_as_docker(model):
    # type: (PathStr) -> str
    with TempDir() as tmp:
        cwd = tmp.path()
        from dbnd_examples.orchestration.examples.wine_quality import container

        source = os.path.dirname(container.__file__)
        work_path = os.path.join(cwd, "package")
        shutil.copytree(source, work_path)
        shutil.copy(model, os.path.join(work_path, "pyfunc"))

        model_version = model.split(os.path.sep)[-2]

        docker_build_cmd = "docker build -t {repo}:{model_version} -f Dockerfile .".format(
            repo=REPOSITORY_NAME, model_version=model_version
        )

        docker_tag_command = "docker tag {repo}:{model_version} {repo}:latest".format(
            repo=REPOSITORY_NAME, model_version=model_version
        )

        commands = [docker_build_cmd, docker_tag_command]
        run_commands(commands, work_path)

    return "{}:{}".format(REPOSITORY_NAME, model_version)
