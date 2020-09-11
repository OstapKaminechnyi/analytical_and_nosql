# The integration between Azure Redis Cache and Azure Event Hubs



**Add .env сonfig file with the next content:**
```
CONSOLE_LOGGING = False
WRITE_TO_REDIS = False

#Redis
REDIS_HOST_NAME = "example.redis.cache.windows.net"
REDIS_KEY = "redis_key"
#Event hub
EVENT_HUB_CONNNECTION_STRING = "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=example;SharedAccessKey=shared_key"
EVENT_HUB_NAME = "example"
CONSUMER_GROUP = "$Default"
```

If CONSOLE_LOGING is True -> logs are going to console, False -> logs are going to Azure Event Hub.

**Installation via `requirements.txt`**:

```shell
$ python3 -m venv .venv
$ source venv/bin/activate
$ pip3 install -r requirements.txt
$ python3 runserver.py
```

**Installation and running via `setup.bash`**:
```shell
bash run.bash
```

## Running an integration

```shell
curl -d "url=https://data.cityofchicago.org/resource/ijzp-q8t2.json" -X POST http://0.0.0.0:8080
```
JSON example:
```
{
   "url":"value1", #must field
   "rows_number": 200, #default:200
   "rows_per_time":100, #default:50
   "offset":0 #default:0
}
```
### Writing results to Redis
If WRITE_TO_REDIS is True:

Redis console:
```
>KEYS *
1) "0"
2) 1) "test"
   2) "file https://data.cityofchicago.org/resource/ijzp-q8t2.json"
>GET "file https://data.cityofchicago.org/resource/ijzp-q8t2.json"
"Started"
>KEYS *
1) "0"
2) 1) "file https://data.cityofchicago.org/resource/ijzp-q8t2.json rows 0-100"

   2) "test"

   3) "file https://data.cityofchicago.org/resource/ijzp-q8t2.json"

>GET "file https://data.cityofchicago.org/resource/ijzp-q8t2.json rows 0-100"
"Completed"
>KEYS *
1) "0"
2) 1) "file https://data.cityofchicago.org/resource/ijzp-q8t2.json rows 0-100"

   2) "test"

   3) "file https://data.cityofchicago.org/resource/ijzp-q8t2.json"

>GET "file https://data.cityofchicago.org/resource/ijzp-q8t2.json"
"Completed"

```

If we will post the same file second time, in Redis we will receive following:
```
>GET "file https://data.cityofchicago.org/resource/ijzp-q8t2.json"
"Retry attempt, ignore a file"
```