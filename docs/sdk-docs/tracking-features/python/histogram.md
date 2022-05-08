---
"title": "Histograms"
---
# Logging Histograms and Statistics

Histograms that include data profiling information can be automatically fetched from Pandas DataFrames, Spark DataFrames and from warehouses like Amazon Redshift and PostgreSQL. See more information at [Histograms](doc:histograms)

By using the `log_dataframe` function, you can enable its advanced logging options: statistics and histograms.
To enable these options, set  the `with_histogram` and `with_stats` parameters to `True`:

```python
from dbnd import log_dataframe

log_dataframe("key",
              data=pandas_df,
              with_stats=True,
              with_histograms=True)
```

Calculating statistics and histograms can take a long time on large data chunks, as it requires analyzing the data. DBND provides a way of specifying which columns you want to analyze.

The following options are available for both `with_histogram` and `with_stats` parameters:

* Iterable[str] - calculate for columns matching names within an iterable (list, tuple, etc.) 
* str - a comma-delimited list of column names
* True - calculate for all columns within a data frame
* False - do not calculate; this behavior is the default

The `LogDataRequest` - can be use for more flexible options, such as calculating only boolean columns. The `LogDataRequest` has the following attributes:

* include_columns - list of column names to include
* exclude_columns - list of column names to exclude
* include_all_boolean, include_all_numeric, include_all_string - select all boolean, numeric, and/or string columns respectively. 

Here is an example of using the `LogDataRequest`:

```python
from dbnd import log_dataframe, LogDataRequest

log_dataframe("customers_data", data,
                  with_histograms=LogDataRequest(include_all_numeric=True,
                                                   exclude_columns=["name", "phone"]))
```

Alternatively, you can use the following helper methods:

`LogDataRequest.ALL()`
`LogDataRequest.ALL_STRING()`
`LogDataRequest.ALL_NUMERIC()`
`LogDataRequest.ALL_BOOLEAN()`
`LogDataRequest.NONE()`



## Enabling Histograms for Python Functions Tracking

 Enable histogram tracking in individual tasks. You can do this by using one of the following methods:
  * Add a decorator with histogram tracking enabled to your task functions:
`@task (<parameter name>=parameter[DataFrame](log_histograms=True)` for task input or `@task(result=output.prod_immutable[DataFrame](log_histograms=True))` for task output.

  * Add the following line to your task code:
`log_dataframe (with_histograms=True)`

[block:html]
{
  "html": "<style>\n  pre {\n      border: 0.2px solid #ddd;\n      border-left: 3px solid #c796ff;\n      color: #0061a6;\n  }\n\n.CodeTabs_initial{\n  /* box shadows with with legacy browser support - just in case */\n    -webkit-box-shadow: 0 10px 6px -6px #777; /* for Safari 3-4, iOS 4.0.2 - 4.2, Android 2.3+ */\n     -moz-box-shadow: 0 10px 6px -6px #777; /* for Firefox 3.5 - 3.6 */\n          box-shadow: 0 10px 6px -6px #777;/* Opera 10.5, IE 9, Firefox 4+, Chrome 6+, iOS 5 */\n  }\n</style>"
}
[/block]