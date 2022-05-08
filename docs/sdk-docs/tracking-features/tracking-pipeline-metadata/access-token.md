---
"title": "Connecting DBND to Databand"
---
Databand Personal Access Token is a security measure that enables Databand to authenticate different services that communicate with the Databand web server. You need to set up a service URL and Databand Personal Access Token to connect your DBND SDK to Databand Application.

Please use the credentials provided to you by the Databand. The credentials will include the following contents:
* Your environment address (i.e. `yourcompanyname.databand.ai`).
* Your Databand Access Token to connect to the environment (you may create more tokens)

You can use these values via environment variables:
```
export DBND__CORE__DATABAND_URL=...
export DBND__CORE__DATABAND_ACCESS_TOKEN=...
```
You can also use dbnd configuration files for that:
``` ini
[core]
databand_url=...
databand_access_token=...
```

You can add these parameters to the tracking context by adding configuration via `conf` parameter of `dbnd_tracking` function. This is not recommended for production usage.

```python
from dbnd import dbnd_tracking

with dbnd_tracking(conf={
                             "core": {
                                       "databand_url": "<databand_url>",
                                       "databand_access_token":"<access_token>",
                                     }
                            }
                      ):
      pass
```

See more at [SDK Configuration](doc:dbnd-sdk-configuration).  If you need an additional Personal Access Token, please follow [this](doc:access-tokens) guide.