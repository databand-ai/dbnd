---
"title": "Metrics"
---
Databand you to define, capture, and monitor custom metrics for further insights into your data pipelines. 

User-defined metrics can represent any quantitative measure of interest such as a business KPI or a data quality measurable - for example, counts, aggregations, summary statistics, or distribution measurements.

# Tracking Metrics with DBND SDK

To log metrics in Databand, use `log_metric()` function which accepts two parameters:
*`metric_name` - a string identifier for a metric
*`metric_value` -  the value for a metric during a given execution

Metric values can be both simple types (e.g., string, integer, bool) as well as complex types (e.g., lists or dicts).

Once defined, each execution of the pipeline will send the metric value to Databand, where you can perform time-series analysis of the metric and create alerts based on the value of a metric for a given pipeline execution. 

Below is an example of `log_metric()`:

``` python
from dbnd import log_metric

def add_values(a: int, b:int):
  sum = a + b
  log_metric('total', sum)
  
  return sum

add_values(12, 16)
```

You can also use `log_metrics` to simultaneously submit multiple metrics.


### Tracking external resources within a specific task
If the value that you want to track is URL, you can use `set_external_resource_urls`. That will log URL in the specific task context.   `set_external_resource_urls(links:dict)` function which accepts one parameter with dictionary of {"key": "URL"} 

``` python
from dbnd._core.tracking.commands import set_external_resource_urls

set_external_resource_urls(
            {"my_resource": "http://some_resource_name.com/path/to/resource/123456789"}
        )
```