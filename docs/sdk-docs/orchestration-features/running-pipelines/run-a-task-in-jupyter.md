---
"title": "Run a Task in Jupyter"
---
Please see how to run a  task from code

## Auto Task Reloading (IPython/Jupyter)

Relevant only for [IPython](https://ipython.org) and [Jupyter Notebook](http://jupyter.org).

It's possible to edit tasks in your IDE and reloading them automatically without restarting the Jupyter kernel or the IPython interpreter.

To enable auto task reloading, run this from the cell or the interpreter:

```
>>> %load_ext autoreload
>>> %autoreload 2
```

After running these lines, you can do the following:

<!-- noqa -->
```python
# use original code
check.dbnd_run(text="/etc/hosts")
```

<!-- noqa -->
```python
# add only_letters parameter in PyCharm and use the new code
check.dbnd_run(text="/etc/hosts", only_letters=True)
```
