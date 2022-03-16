---
"title": "Run a Task in Python"
---
We want DBND to fit your preferred way of doing work, regardless of the hardware or the IDE you are using to develop your code. 

You can use DBND within tools like [Jupyter Notebook](https://jupyter.org), [IPython](https://ipython.org), [PyCharm](https://www.jetbrains.com/pycharm/), or simply from a Python script.

## How To Run a Task in Simple Python

If you want to trigger your execution programmatically as part of a broader Python process, you can define a DBND run using a simple python code:
```python
@task
def calculate_alpha(alpha = 0.5):
    return alpha

class CalculateBeta(PythonTask):
    beta = parameter.value(0.1)
    result = output

    def run(self):
        self.result.write(self.beta)

if __name__ == '__main__':
    calculate_alpha.dbnd_run(alpha=0.4)
    CalculateBeta(beta=0.15).dbnd_run()
```

To get a full list of options supported by a *run* command, Run `dbnd run --help`.