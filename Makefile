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
#AIRFLOW_VERSION ?= 1.10.15
export AIRFLOW_VERSION ?= 2.2.5
export AIRFLOW_VERSION_PREFIX = airflow_$(subst .,_,${AIRFLOW_VERSION})

### VIRTUAL ENVs NAMES
VENV_TARGET_NAME ?= dbnd-${PYTHON_VERSION_PREFIX}
VENV_TRACKING_AIRFLOW_TARGET_NAME ?= dbnd-${PYTHON_VERSION_PREFIX}-tracking-${AIRFLOW_VERSION_PREFIX}
VENV_RUN_AIRFLOW_TARGET_NAME ?= dbnd-${PYTHON_VERSION_PREFIX}-run-${AIRFLOW_VERSION_PREFIX}


prj_tracking = \
			modules/dbnd \
			plugins/dbnd-mlflow \
			plugins/dbnd-postgres \
			plugins/dbnd-redshift \
			plugins/dbnd-tensorflow \
			plugins/dbnd-spark \
			plugins/dbnd-snowflake

prj_tracking_monitors = \
           modules/dbnd \
           modules/dbnd-dbt-monitor \
           modules/dbnd-datastage-monitor


prj_tracking_airflow = \
           modules/dbnd \
           modules/dbnd-airflow \
           modules/dbnd-airflow-monitor\
           plugins/dbnd-airflow-auto-tracking \
           plugins/dbnd-airflow-export



prj_dbnd_run = \
            modules/dbnd \
            modules/dbnd-airflow \
            plugins/dbnd-airflow-versioned-dag \
            plugins/dbnd-aws  \
            plugins/dbnd-azure \
			plugins/dbnd-databricks \
			plugins/dbnd-docker \
			plugins/dbnd-hdfs \
			plugins/dbnd-gcp \
			plugins/dbnd-tensorflow \
			plugins/dbnd-spark \
			plugins/dbnd-qubole \
			\
			plugins/dbnd-test-scenarios

# LIST of packages to be distributed
prj_dist := \
		modules/dbnd \
		modules/dbnd-airflow \
		plugins/dbnd-spark \
		\
		modules/dbnd-airflow-monitor \
		plugins/dbnd-airflow-auto-tracking \
		plugins/dbnd-airflow-export \
		\
		modules/dbnd-dbt-monitor \
		modules/dbnd-datastage-monitor\
		\
		plugins/dbnd-mlflow \
		plugins/dbnd-luigi \
		plugins/dbnd-postgres \
		plugins/dbnd-redshift \
		\
		plugins/dbnd-airflow-versioned-dag \
		plugins/dbnd-aws  \
		plugins/dbnd-azure \
		plugins/dbnd-databricks \
		plugins/dbnd-docker \
		plugins/dbnd-hdfs \
		plugins/dbnd-gcp \
		plugins/dbnd-tensorflow \
		plugins/dbnd-snowflake \
		plugins/dbnd-qubole\
		\
		plugins/dbnd-test-scenarios\
		\
		examples


# https://reproducible-builds.org/docs/source-date-epoch/
SOURCE_DATE_EPOCH=1577836800  # 2020-01-01T00:00:00Z

##@ Test
.PHONY: lint test test-all-py39 test-manifest coverage coverage-open pre-commit

lint: ## Check style with flake8.
	tox -e pre-commit,lint
	(cd docs; make validate-doc-style)

test: ## Run tests quickly with the default Python.
	py.test modules/dbnd/test_dbnd
	tox -e pre-commit,lint

test-all-py39: ## Run tests on every python package with tox.
	for m in $(prj_dist) ; do \
		echo "Testing '$$m'..." ;\
		(cd $$m && tox -e py39) ;\
	done

test-manifest: ## Run minifest tests on every python package with tox.
	set -e;\
	for m in $(prj_dist) ; do \
		echo "Building '$$m'..." ;\
		(cd $$m && tox -e manifest) ;\
	done

coverage: ## Check code coverage quickly with the default Python.
	py.test --cov-report=html --cov=databand  tests

coverage-open: coverage ## Open code coverage in a browser.
	$(BROWSER) htmlcov/index.html

pre-commit: ## Run pre-commit checks.
	tox -e pre-commit


##@ Distribution
.PHONY: dist dist-python dist-java clean clean-python

dist:  ## Cleanup and build packages for all python modules (java packages are excluded).
	make clean-python
	make dist-python
	ls -l dist

__dist-python-module:  ## (Hidden target) Build a single python module.
	echo "Building '${MODULE}'..." ;
	# Build *.tar.gz and *.whl packages:
	(cd ${MODULE} && python setup.py sdist bdist_wheel);

	# Generate requirements...
	python etc/scripts/generate_requirements.py \
		--wheel ${MODULE}/dist/*.whl \
		--output ${MODULE}/dist/$$(basename ${MODULE}).requirements.txt \
		--third-party-only \
		--extras airflow,tests,composer,bigquery \
		--separate-extras;

	# Move to root dist dir...
	mv ${MODULE}/dist/* dist-python;

dist-python:  ## Build all python modules.
	rm -Rf dist-python
	mkdir -p dist-python;
	set -e;\
	for m in $(prj_dist); do \
		MODULE=$$m make __dist-python-module;\
	done;

	@echo "\n\nCheck if dbnd.requirements.txt in repo is updated"
	@# add newline at the end of dist-python/dbnd.requirements.txt to match
	@echo "" >> dist-python/dbnd.requirements.txt
	@cmp -s dist-python/dbnd.requirements.txt modules/dbnd/dbnd.requirements.txt && \
		echo "dbnd.requirements.txt is expected" || \
		(echo "Error: dbnd.requirements.txt files doesn't match" && exit 1);

	# create databand package
	python setup.py sdist bdist_wheel
	mv dist/* dist-python/

	# Running stripzip (CI only)...
	if test -n "${CI_COMMIT_SHORT_SHA}"; then stripzip ./dist-python/*.whl; fi;

	cp examples/requirements.txt dist-python/dbnd-examples.requirements.txt
	echo SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}

	@# Calculate md5 for generated packages (with osx and linux support)
	@export MD5=md5; if ! command -v md5 &> /dev/null; then export MD5=md5sum; fi;\
	for file in dist-python/*; do $$MD5 $$file || true; done > dist-python/hash-list.txt



dist-java:  ## Build dbnd-java modules.
	(cd modules/dbnd-java/ && ./gradlew build)

clean: ## Remove all build, test, coverage and Python artifacts.
	@make clean-python

	@echo "Removing python execution artifacts..."
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

	@echo "Removing test and coverage artifacts..."
	find . -name ".tox" -type d -exec rm -r "{}" \;
	find . -name ".pytest_cache" -type d -exec rm -r "{}" \;
	find . -name ".coverage" -type d -exec rm -r "{}" \;
	find . -name "htmlcov" -type d -exec rm -r "{}" \;

clean-python:  ## Remove bulid artifacts.
	@echo "Removing build artifacts..."
	pwd
	rm -rf modules/*/build
	rm -rf modules/*/dist
	rm -rf plugins/*/build
	rm -rf plugins/*/dist
	find . -name "eggs" -type d -exec rm -r "{}" \;
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

##@ Development
.PHONY: uninstall-dev
uninstall-dev:  ## (Hidden target) Remove all dbnd modules from the current virtual environment.
	pip uninstall databand -y || true
	pip freeze | grep "dbnd" | egrep -o '#egg=dbnd[a-z_]*' | egrep -o 'dbnd[a-z_]*' | (xargs pip uninstall -y || true)


############################
##@ Development: core tracking (without Airflow or any other heavy deps)

.PHONY: install-dev pip-compile __is_venv_activated create-venv
__is_venv_activated:  ## (Hidden target) check if correct virtual env is activated
	. ./etc/scripts/devenv-utils.sh; _validate_python_venv_name ${VENV_TARGET_NAME}

create-venv:  ## Create virtual env for dbnd-core
	. ./etc/scripts/devenv-utils.sh; _create_virtualenv ${VENV_TARGET_NAME}

pip-compile: __is_venv_activated ## Regenerate deps and constrains
	pip-compile --resolver backtracking requirements/requirements-dev.in

install-dev: __is_venv_activated  ## Install all modules, except Airflow, in editable mode to the active Python's site-packages.
	pip-sync requirements/requirements-dev.txt


############################
##@ Development: tracking for Apache Airflow
.PHONY: __is_venv_activated__tracking_airflow \
         tracking-airflow--create-venv \
         tracking-airflow--dist-python \
         tracking-airflow--install-dev
REQUIREMENTS_FILE_TRACKING_AIRFLOW=requirements/requirements-dev-tracking-airflow-${PYTHON_VERSION}-airflow-${AIRFLOW_VERSION}.txt


__is_venv_activated__tracking_airflow:  ## (Hidden target) check if correct virtual env is activated
	. ./etc/scripts/devenv-utils.sh; _validate_python_venv_name ${VENV_TRACKING_AIRFLOW_TARGET_NAME}

tracking-airflow--create-venv:  ## Create virtual env
	. ./etc/scripts/devenv-utils.sh; _create_virtualenv ${VENV_TRACKING_AIRFLOW_TARGET_NAME}


tracking-airflow--dist-python:  ## Build only essential airflow tracking modules.
	mkdir -p dist-python;
	set -e;\
	for m in $(prj_tracking_airflow) ; do \
		MODULE=$$m make __dist-python-module;\
	done;


tracking-airflow--install-dev: __is_venv_activated__tracking_airflow  ## Installs all relevant dbnd-core modules in editable mode to the active Python's site-packages + Apache Airflow.
	pip-sync ${REQUIREMENTS_FILE_TRACKING_AIRFLOW}

tracking-airflow--pip-compile: __is_venv_activated__tracking_airflow  ## Regenerate deps and constrains
	pip-compile -v --resolver backtracking requirements/requirements-dev-tracking-airflow.in \
	    -o ${REQUIREMENTS_FILE_TRACKING_AIRFLOW}


###############
##@ Development: Orchestration with Apache Airflow (dbnd-run)
.PHONY: __run_airflow__is_venv_activated \
         run-airflow--pip-compile \
         run-airflow--create-venv \
         run-airflow--install-dev \
         run-airflow--dist-python

REQUIREMENTS_FILE_RUN_AIRFLOW=requirements/requirements-dev-run-airflow-${PYTHON_VERSION}-airflow-${AIRFLOW_VERSION}.txt

__run_airflow__is_venv_activated:  ## (Hidden target) check if correct virtual env is activated
	. ./etc/scripts/devenv-utils.sh; _validate_python_venv_name "${VENV_RUN_AIRFLOW_TARGET_NAME}"

run-airflow--pip-compile: __run_airflow__is_venv_activated ## Regenerate deps and constrains
	pip-compile -v --resolver backtracking requirements/requirements-dev-run-airflow.in \
	     -o ${REQUIREMENTS_FILE_RUN_AIRFLOW}

run-airflow--create-venv:  ## Create virtual env
	@echo "Current Airflow Version: ${AIRFLOW_VERSION}"
	. ./etc/scripts/devenv-utils.sh; _create_virtualenv "${VENV_RUN_AIRFLOW_TARGET_NAME}"

run-airflow--install-dev: __run_airflow__is_venv_activated  run-airflow--install-apache-airflow## Installs Airflow + all dbnd-core modules in editable mode to the active Python's site-packages.
	pip-sync ${REQUIREMENTS_FILE_RUN_AIRFLOW}

run-airflow--dist-python:  ## Build only essential dbnd-run modules.
	mkdir -p dist-python;
	set -e;\
	for m in $(prj_dbnd_run) ; do \
		MODULE=$$m make __dist-python-module;\
	done;
