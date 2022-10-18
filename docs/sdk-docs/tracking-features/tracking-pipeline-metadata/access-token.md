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

## `[core]` Configuration Section Parameter Reference
- `databand_url` - Set the tracker URL which will be used for creating links in console logs.
- `databand_access_token` - Set the personal access token used to connect to the Databand web server.
- `extra_default_headers` - Specify extra headers to be used as defaults for databand_api_client.
- `tracker` - Set the Tracking Stores to be used.
- `tracker_api` - Set the Tracker Channels to be used by 'api' store.
- `debug_webserver` - Enable collecting the webserver's logs for each api-call on the local machine. This requires that the web-server supports and allows this.
- `silence_tracking_mode` - Enables silencing the console when in tracking mode.
- `tracker_raise_on_error` - Enable raising an error when failed to track data.
- `remove_failed_store` - Enable removal of a tracking store if it fails.
- `max_tracking_store_retries` - Set maximum amount of retries allowed for a single tracking store call if it fails.
- `client_session_timeout` - Set number of minutes to recreate the api client's session.
- `client_max_retry` - Set the maximum amount of retries on failed connection for the api client.
- `client_retry_sleep` - Set the amount of sleep time in between retries of the API client.
- `user_configs` - Set the config used for creating tasks from user code.
- `user_init` - This runs in every DBND process with System configuration in place. This is called in DatabandContex after entering initialization steps.
- `user_driver_init` - This runs in driver after configuration initialization. This is called from DatabandContext when entering a new context(dbnd_on_new_context)
- `user_code_on_fork` - This runs in a sub process, on parallel, kubernetes, or external modes.
- `plugins` - Specify which plugins should be loaded on Databand context creations.
- `allow_vendored_package` - Enable adding the `dbnd/_vendor_package` module to your system path.
- `fix_env_on_osx` - Enable adding `no_proxy=*` to environment variables, fixing issues with multiprocessing on OSX.
- `environments` - Set a list of enabled environments.
- `dbnd_user` - Set which user should be used to connect to the DBND web server. This is deprecated!
- `dbnd_password` - Set what password should be used to connect to the DBND web server. This is deprecated!
- `tracker_url` - Set the tracker URL to be used for creating links in console logs. This is deprecated!


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
