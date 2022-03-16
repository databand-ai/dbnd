---
"title": "Run a Task in PyCharm"
---
## How to Run a Task in PyCharm

PyCharm is a common IDE for python programming and a popular tool for machine learning engineers and data scientists.

To run DBND tasks and pipelines from PyCharm, create a standard Python run configuration:

* select "Module name" at "Choose Target to run" (instead of "Script Path")
* set `dbnd` as a module name
* set `run <your task name> <extra args>`  at Parameters field

![IDE](https://files.readme.io/02739b2-IDE.png)

Run ``dbnd run --help`` to get a full list of options supported by a *run* command