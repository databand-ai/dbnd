---
"title": "Tracking Data Objects Configuration"
---
## Controlling parameters logging within Decorated functions

Databand's performant logging ("zero-computational-cost") option helps users save computational resources and protect against reporting of sensitive data (like full data previews).  

Logging data processes and full data quality reports in DBND can be resource-intensive. However, explicitly turning off all calculations for `log_value_size`, `log_value_schema`, `log_value_stats`, `log_value_preview`, `log_value_preview_max_len`, `log_value_meta` (the former approach) will result in valuable metrics not being tracked at all. To help users better manage performance and visibility needs, it's now possible to turn selectively throttle the calculations and logging of metadata through a zero-computational-cost approach.

Using a new configuration, you can decide if want to log certain information or not with the help of `value_reporting_strategy`. `value_reporting_strategy` changes nothing in your code, but acts as a guard (or fuse) before the value calculation code gets to execution:

``` ini
[tracking]
value_reporting_strategy=SMART
```

These are the 3 options available: 

*  **"ALL => no limitations"** - no restrictions for logging. All the log_value_ types are going to be on: `log_value_size`, `log_value_scheme`, `log_value_stats`, `log_value_preview`, `log_value_preview_max_len`, `log_value_meta`.
 
*  **"SMART => restrictions on lazy evaluation types”** - with SMART, for types like Spark calculations are only performed as required. The calculations for lazy evaluation types are restricted and even if you have `log_value_preview` set to True, with the SMART strategy on, the Spark previews will not be logged.

*  **"NONE => limit everything"** that doesn’t allow logging of anything expensive or potentially problematic. This can be useful if you have some or many values that constitute private and sensitive information, and you don’t want it to be logged. 

Most users will benefit from using the SMART option for logging.