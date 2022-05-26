.DEFAULT_GOAL:=help
SHELL := /bin/bash

##@ Helpers
.PHONY: help


CURRENT_PY_VERSION = $(shell python -c "import sys; print('{0}.{1}'.format(*sys.version_info[:2]))")
# use env value if exists
AIRFLOW_VERSION ?= 1.10.12
#AIRFLOW_VERSION = 2.2.4


help:  ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z].[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

prj_modules = modules/dbnd modules/dbnd-airflow modules/dbnd-airflow-monitor

prj_plugins = plugins/dbnd-aws  \
			plugins/dbnd-azure \
			plugins/dbnd-airflow-auto-tracking \
			plugins/dbnd-airflow-versioned-dag \
			plugins/dbnd-airflow-export \
			plugins/dbnd-databricks \
			plugins/dbnd-docker \
			plugins/dbnd-hdfs \
			plugins/dbnd-gcp \
			plugins/dbnd-mlflow \
			plugins/dbnd-luigi \
			plugins/dbnd-postgres \
			plugins/dbnd-redshift \
			plugins/dbnd-tensorflow \
			plugins/dbnd-test-scenarios \
			plugins/dbnd-spark \
			plugins/dbnd-snowflake \
			plugins/dbnd-qubole

prj_plugins_spark  = plugins/dbnd-spark \
				plugins/dbnd-databricks \
				plugins/dbnd-qubole


prj_dbnd_tracking_slim = modules/dbnd modules/dbnd-airflow \
           modules/dbnd-airflow-monitor\
          plugins/dbnd-airflow-auto-tracking

prj_dbnd_run = modules/dbnd modules/dbnd-airflow \
            modules/dbnd-airflow-monitor  \
            plugins/dbnd-redshift  \
            plugins/dbnd-aws  \
            plugins/dbnd-airflow-auto-tracking  \
			plugins/dbnd-azure \
			plugins/dbnd-airflow-versioned-dag \
			plugins/dbnd-databricks \
			plugins/dbnd-docker \
			plugins/dbnd-hdfs \
			plugins/dbnd-gcp \
			plugins/dbnd-tensorflow \
			plugins/dbnd-test-scenarios \
			plugins/dbnd-spark \
			plugins/dbnd-qubole

prj_examples = examples
prj_test = plugins/dbnd-test-scenarios

prj_dev =$(prj_modules) $(prj_plugins) $(prj_examples) $(prj_test)

prj_dist = $(prj_modules) $(prj_plugins) $(prj_examples)

# https://reproducible-builds.org/docs/source-date-epoch/
SOURCE_DATE_EPOCH=1577836800  # 2020-01-01T00:00:00Z


##@ Test
.PHONY: lint test test-all-py36 test-manifest coverage coverage-open pre-commit

lint: ## Check style with flake8.
	tox -e pre-commit,lint
	(cd docs; make validate-doc-style)

test: ## Run tests quickly with the default Python.
	py.test modules/dbnd/test_dbnd
	tox -e pre-commit,lint

test-all-py36: ## Run tests on every python package with tox.
	for m in $(prj_dev) ; do \
		echo "Testing '$$m'..." ;\
		(cd $$m && tox -e py36) ;\
	done

test-manifest: ## Run minifest tests on every python package with tox.
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
		--extras airflow,airflow_1_10_8,airflow_1_10_9,airflow_1_10_10,airflow_1_10_12,airflow_1_10_15,airflow_2_0_2,airflow_2_2_3,airflow_2_3_0,tests,composer,bigquery \
		--separate-extras;

	# Move to root dist dir...
	mv ${MODULE}/dist/* dist-python;

dist-python:  ## Build all python modules.
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


dist-python-tracking-slim:  ## Build only essential airflow trackign modules.
	mkdir -p dist-python;
	set -e;\
	for m in $(prj_dbnd_tracking_slim); do \
		MODULE=$$m make __dist-python-module;\
	done;

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
.PHONY: install-dev

VENV_TARGET ?= dbnd-core
VENV_TARGET_NAME ?= venv-${VENV_TARGET}-py$(subst .,,${CURRENT_PY_VERSION})



create-venv:  ## Create virtual env for dbnd-core
	@echo 'Virtual env "${VENV_TARGET_NAME}" with python version from a current shell is going to be created'
	@echo "Current python version: "
	@python -c "import sys; print(sys.version)"
	@read -p "Are you sure you want to proceed? " -n 1 -r; [[ "Yy" != *"$$REPLY"* ]] && exit 1; echo;
	pyenv virtualenv ${VENV_TARGET_NAME}

__is_venv_activated:  ## (Hidden target) check if correct virtual env is activated
	@export CURRENT_VENV_NAME=$$(basename ${VIRTUAL_ENV}); \
	if [[ "$$CURRENT_VENV_NAME" != *"${VENV_TARGET}"* ]]; \
	then \
		if [[ -z "${VIRTUAL_ENV}" ]]; \
		then \
			echo "Virtual env is not activated, activate ${VENV_TARGET_NAME}"; \
		else \
			echo "Looks like wrong virtual env is activated, ${VENV_TARGET_NAME} is expected"; \
		fi; \
		\
		read -p "Are you sure you want to proceed? " -n 1 -r; \
		[[ "Yy" != *"$$REPLY"* ]] && exit 1; \
	fi; \
	echo; \
	echo Virtual env check passed, current venv is $$CURRENT_VENV_NAME


install-airflow: ## Installs Airflow with strictly pinned dependencies into current virtual environment.
	@make __is_venv_activated; \
	echo Will install Airflow==${AIRFLOW_VERSION}; \

	pip install apache-airflow==${AIRFLOW_VERSION} -c https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${CURRENT_PY_VERSION}.txt


install-dev: ## Installs Airflow + all dbnd-core modules in editable mode to the active Python's site-packages.
	@make __is_venv_activated; \
  	make install-airflow;\
  	make install-dev-without-airflow


install-dev-without-airflow: ## Install all modules, except Airflow, in editable mode to the active Python's site-packages.
	@make __is_venv_activated
	make __uninstall-dev


	pip install \
		pylint==2.13.5 mypy==0.942 \
		pip==21.3.1  # python 3.6

	set -e; \
	for m in $(prj_dbnd_run) ; do \
		all_reqs="$$all_reqs -e $$m"; \
		export CURRENT_DEPS=$$all_reqs; \
	done; \
	echo "Running all deps installation at once:";  \
	echo pip install $$all_reqs; \
	pip install $$all_reqs; \

__uninstall-dev:  ## (Hidden target) Remove all dbnd modules from the current virtual environment.
	pip uninstall databand -y || true
	pip freeze | grep "dbnd" | egrep -o '#egg=dbnd[a-z_]*' | egrep -o 'dbnd[a-z_]*' | (xargs pip uninstall -y || true)
