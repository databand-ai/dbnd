import six


if six.PY3:
    from dbnd_examples.orchestration.examples.wine_quality import (
        wine_quality_decorators_py3 as wine_quality_decorators,
    )
else:
    pass
