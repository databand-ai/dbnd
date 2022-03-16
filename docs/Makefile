help:
	@echo The commands here are used to build html docs and upload them to readme automatically.
	@echo validate-doc-style: Ensures all docstrings in dbnd exit and use the Google Python Style Guide.
	@echo build-sphinx-html: Builds the html docs with Sphinx
	@echo readme-upload: Builds the html docs with Sphinx and then automatically uploads them to readme.
	@echo Be sure to call this with apikey={APIKEY}, where {APIKEY} is the readme API key.

readme-upload: build-sphinx-html ## Build an up-to-date version of the docs and upload it to readme. call this with apikey={APIKEY}
ifdef apikey
	python upload_docs.py $(apikey)
else
	echo "Error! call with apikey={APIKEY}"
	exit 1
endif

build-sphinx-html: ## Build an up-to-date version of the docs.
	python build_api_reference.py

validate-doc-style: ## Ensures all docstrings in dbnd exist and use the Google Python Style Guide
	python check_valid_docstring.py
