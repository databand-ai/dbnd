# Databand Monitor

Databand Monitor is a module providing functionality shared across *monitors* within Databand system, like Databand Airflow Monitor.

## Installation with setup tools

```bash
cd modules/dbnd-monitor
pip install -e .
```

## Usage

```python
from dbnd_monitor.base_integration import BaseIntegration
from dbnd_monitor.base_monitor_config import BaseMonitorConfig
from dbnd_monitor.multiserver_monitor_starter import monitor_cmd


class ExampleMonitorConfig(BaseMonitorConfig):
    ...


class ExampleIntegartion(BaseIntegration):
    MONITOR_TYPE = "example_monitor"
    CONFIG_CLASS = ExampleMonitorConfig
    config: ExampleMonitorConfig


monitor_cmd(obj=ExampleIntegartion)
```
