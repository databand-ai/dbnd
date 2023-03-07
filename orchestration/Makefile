.DEFAULT_GOAL:=help
SHELL := /bin/bash

##@ Helpers
.PHONY: help

help:  ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z].[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

####
### BUILD ENVIRONMENT VARIABLES
export PYTHON_VERSION = $(shell python -c "import sys; print('{0}.{1}'.format(*sys.version_info[:2]))")
export PYTHON_VERSION_PREFIX = py$(subst .,,${PYTHON_VERSION})

# use env value if exists
export AIRFLOW_VERSION ?= 2.2.5
export AIRFLOW_VERSION_PREFIX = airflow_$(subst .,_,${AIRFLOW_VERSION})

### VIRTUAL ENVs NAMES
VENV_RUN_AIRFLOW_TARGET_NAME ?= dbnd-${PYTHON_VERSION_PREFIX}-run-${AIRFLOW_VERSION_PREFIX}


prj_dbnd_run = \
            ../modules/dbnd \
            dbnd-run \
            dbnd-aws  \
            dbnd-azure \
			dbnd-databricks \
			dbnd-docker \
			dbnd-hdfs \
			dbnd-gcp \
			dbnd-spark \
			dbnd-qubole \
			\
			dbnd-test-scenarios


# https://reproducible-builds.org/docs/source-date-epoch/
SOURCE_DATE_EPOCH=1577836800  # 2020-01-01T00:00:00Z


###############
##@ Development: Orchestration with Apache Airflow (dbnd-run)
.PHONY: __run_airflow__is_venv_activated \
         pip-compile \
         create-venv \
         install-dev \
         dist-python

REQUIREMENTS_FILE_RUN_AIRFLOW=requirements/requirements-dev-run-airflow-${PYTHON_VERSION}-airflow-${AIRFLOW_VERSION}.txt

__run_airflow__is_venv_activated:  ## (Hidden target) check if correct virtual env is activated
	. ../etc/scripts/devenv-utils.sh; _validate_python_venv_name "${VENV_RUN_AIRFLOW_TARGET_NAME}"

pip-compile: __run_airflow__is_venv_activated ## Regenerate deps and constrains
	pip-compile -v --resolver backtracking requirements/requirements-dev-run-airflow.in \
	     -o ${REQUIREMENTS_FILE_RUN_AIRFLOW}

create-venv:  ## Create virtual env
	@echo "Current Airflow Version: ${AIRFLOW_VERSION}"
	@if ! python -V | grep -w -e 3.9; then \
		echo "Python 3.9 is required. Otherwise Airflow 2.3.4 will not have correct constraints / docker images"; \
		exit 1; \
	fi
	. ../etc/scripts/devenv-utils.sh; _create_virtualenv "${VENV_RUN_AIRFLOW_TARGET_NAME}"
	@echo -e "Use via:\n    pyenv deactivate && pyenv activate ${VENV_RUN_AIRFLOW_TARGET_NAME} && pip install pip-tools"


install-dev: __run_airflow__is_venv_activated ## Installs Airflow + all dbnd-core modules in editable mode to the active Python's site-packages.
	pip-sync ${REQUIREMENTS_FILE_RUN_AIRFLOW}
