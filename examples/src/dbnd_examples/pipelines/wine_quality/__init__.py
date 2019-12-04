import six

if six.PY3:
    from dbnd_examples.pipelines.wine_quality import (
        wine_quality_decorators_py3 as wine_quality_decorators,
    )
else:
    from dbnd_examples.pipelines.wine_quality import (
        wine_quality_decorators_py2 as wine_quality_decorators,
    )
