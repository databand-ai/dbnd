.PHONY: clean-pyc clean-build docs clean docs-open coverage coverage-open install-dev clean-egg pre-commit

prj_modules = modules/dbnd modules/dbnd-airflow modules/dbnd-airflow-monitor


prj_plugins = plugins/dbnd-aws  \
			plugins/dbnd-azure \
			plugins/dbnd-airflow-auto-tracking \
			plugins/dbnd-airflow-versioned-dag \
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

prj_examples = examples
prj_test = plugins/dbnd-test-scenarios

prj_dev =$(prj_modules) $(prj_plugins) $(prj_examples) $(prj_test)

prj_dist = $(prj_modules) $(prj_plugins) $(prj_examples)

help:  ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)



clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts"

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +



clean-egg:
	find . -name '*.egg-info' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/

lint: ## check style with flake8
	tox -e pre-commit,lint

test: ## run tests quickly with the default Python
	py.test modules/dbnd/test_dbnd
	tox -e pre-commit,lint

test-all-py36: ## run tests on every Python version with tox
	for m in $(prj_dev) ; do \
		echo "Testing '$$m'..." ;\
		(cd $$m && tox -e py36) ;\
	done

test-manifest:
	mkdir -p dist
	for m in $(prj_dist) ; do \
		echo "Building '$$m'..." ;\
		(cd $$m && tox -e manifest) ;\
	done


coverage: ##  check code coverage quickly with the default Python
	py.test --cov-report=html --cov=databand  tests

coverage-open: coverage
	$(BROWSER) htmlcov/index.html


pre-commit: ## run pre-commit checks
	tox -e pre-commit

docs: ## generate Sphinx HTML documentation, including API docs
	tox -e docs

docs-open: docs ## docs + open documentation in default browser
	$(BROWSER) docs/_build/html/index.html

servedocs: docs
	watchmedo shell-command -p '*.md' -c '$(MAKE) -C docs html' -R -D .



##@ Distribution
.PHONY: release clean-dist dist-modules-dirty dist-modules dist dist-java

release:  ## package and upload a release
	make dist
	twine upload -r databand dist/*

clean-dist:  ##
	rm -fr dist
	for m in $(prj_dist) ; do \
		echo "Cleaning '$$m/dist" ;\
		rm -fr $$m/dist;\
		echo "Cleaning '$$m/build" ;\
		rm -fr $$m/build;\
	done

dist-module-dirty:  ## Build a single module
	echo "Building '${MODULE}'..." ;
	# Build *.tar.gz and *.whl packages:
	(cd ${MODULE} && python setup.py sdist bdist_wheel);

	# Generate requirements...
	python etc/scripts/generate_requirements.py \
		--wheel ${MODULE}/dist/*.whl \
		--output ${MODULE}/dist/$$(basename ${MODULE}).requirements.txt \
		--third-party-only \
		--extras airflow,airflow_1_10_8,airflow_1_10_9,airflow_1_10_10,airflow_1_10_12,airflow_1_10_15,airflow_2_0_2,tests,composer,mysql,bigquery \
		--separate-extras;

	# Move to root dist dir...
	mv ${MODULE}/dist/* dist;\

dist-modules-dirty:  ## Build all modules
	# Install python-stripzip (CI only):
	if test -n "${CI_COMMIT_SHORT_SHA}"; then pip install python-stripzip; fi;

	mkdir -p dist
	set -e;\
	for m in $(prj_dist); do \
		MODULE=$$m make dist-module-dirty;\
	done;\

	# create databand package
	python setup.py sdist bdist_wheel

	# Running stripzip (CI only)...
	if test -n "${CI_COMMIT_SHORT_SHA}"; then stripzip ./dist/*.whl; fi;

	cp examples/requirements.txt dist/dbnd-examples.requirements.txt
	echo SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}

	@# Calculate md5 for generated packages (with osx and linux support)
	@export MD5=md5; if ! command -v md5 &> /dev/null; then export MD5=md5sum; fi;\
	for file in dist/*; do $$MD5 $$file; done > dist/hash-list.txt



dist:  ## package all others
	make clean-dist
	make dist-modules-dirty
	ls -l dist

dist-java:  ## build java project
	(cd modules/dbnd-java/ && ./gradlew build)

##@ Development
.PHONY: install-dev install-dev-spark
install-dev: ## install all modules in editable mode to the active Python's site-packages
	for m in $(prj_dev) ; do \
		echo "Installing '$$m'..." ;\
		(cd $$m && pip install -e .) ;\
	done

install-dev-spark: install-dev ## install the package to the active Python's site-packages
	for m in $(prj_plugins_spark) ; do \
		echo "Installing '$$m'..." ;\
		(cd $$m && pip install -e .) ;\
	done
