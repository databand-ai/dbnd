.PHONY: clean-pyc clean-build docs clean docs-open coverage coverage-open install-dev clean-egg pre-commit

prj_modules = modules/dbnd modules/dbnd-airflow
prj_plugins = 	plugins/dbnd-aws  \
          	plugins/dbnd-azure \
			plugins/dbnd-databricks \
			plugins/dbnd-docker \
			plugins/dbnd-hdfs \
          	plugins/dbnd-gcp \
          	plugins/dbnd-spark \
          	plugins/dbnd-test-scenarios

prj_dist = $(prj_modules) $(prj_plugins)

prj_examples = examples
prj_test = plugins/dbnd-test-scenarios

prj_dev_testable =$(prj_modules) $(prj_plugins) $(prj_examples)
prj_dev = $(prj_dev_testable) $(prj_test)


help:
	@echo "clean - remove all build, test, coverage and Python artifacts"
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-test - remove test and coverage artifacts"
	@echo "lint - check style with flake8"
	@echo "test - run tests quickly with the default Python"
	@echo "test-all - run tests on every Python version with tox"
	@echo "test-gitlab-ci - test gitlab ci/cd yaml"
	@echo "coverage - check code coverage quickly with the default Python"
	@echo "pre-commit - run pre-commit checks"
	@echo "docs - generate Sphinx HTML documentation, including API docs"
	@echo "docs-open - docs + open documentation in default browser"
	@echo "release - package and upload a release"
	@echo "dist - package"
	@echo "install-site-packages - install the package to the active Python's site-packages"
	@echo "install-dev - install all modules in editable mode to the active Python's site-packages"
	@echo "build-modules - build all modules"

clean: clean-build clean-pyc clean-test

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +


clean-egg:
	find . -name '*.egg-info' -exec rm -fr {} +

clean-test:
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/

lint:
	tox -e flake8

test:
	py.test modules/dbnd/test_dbnd
	tox -e pre-commit,lint

test-all-py36:
	for m in $(prj_dev_testable) ; do \
		echo "Testing '$$m'..." ;\
		(cd $$m && tox -e py36) ;\
	done

test-gitlab-ci:
	python etc/scripts/validate_gitlab_ci_yml.py --file ./.gitlab-ci.yml


test-manifest:
	mkdir -p dist
	for m in $(prj_dist) ; do \
		echo "Building '$$m'..." ;\
		(cd $$m && tox -e manifest) ;\
	done


coverage:
	py.test --cov-report=html --cov=databand  tests

coverage-open: coverage
	$(BROWSER) htmlcov/index.html

pre-commit:
	tox -e pre-commit

docs:
	tox -e docs

docs-open: docs
	$(BROWSER) docs/_build/html/index.html

servedocs: docs
	watchmedo shell-command -p '*.md' -c '$(MAKE) -C docs html' -R -D .

dist: clean
	python setup.py sdist
	python setup.py bdist_wheel
	ls -l dist

release: dist
	twine upload -r databand dist/*

dist-modules-dirty:
	mkdir -p dist
	python setup.py sdist bdist_wheel
	for m in $(prj_dist) ; do \
		echo "Building '$$m'..." ;\
		(cd $$m && python setup.py sdist bdist_wheel) ;\
		mv $$m/dist/* dist;\
	done

dist-modules: clean dist-modules-dirty


install-dev:
	for m in $(prj_dev_testable) ; do \
		echo "Installing '$$m'..." ;\
		(cd $$m && pip install -e .) ;\
	done
