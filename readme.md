# l2met

Convert a formatted log stream into metrics.

* [Synopsis](#synopsis)
* [Log Conventions](#log-conventions)
* [API](#api)
* [Setup](#setup)
* [Architecture](#architecture)

## Synopsis

L2met receives HTTP requests that contain a body of rfc5424 formatted data. Commonly data is drained into l2met by [logplex](https://github.com/heroku/logplex) or [log-shuttle](https://github.com/ryandotsmith/log-shuttle).

Once data is delivered, l2met extracts and parses the individual log lines using the [log conventions](#log-conventions) and then stores the data in redis so that outlets can read the data and build metrics. The librato_outlet is the most popular and will put all of your metrics into your librato account. See the [setup](#setup) section to get started.

## Log Conventions

L2met uses convention over configuration to build metrics.

```
measure="myapp" view=20 db=10
```
Metrics Produced:

* myapp.view.min
* myapp.view.median
* myapp.view.perc95
* myapp.view.perc99
* myapp.view.max
* myapp.view.mean
* myapp.view.last
* myapp.view.count
* myapp.view.sum

* myapp.db.min
* myapp.db.median
* myapp.db.perc95
* myapp.db.perc99
* myapp.db.max
* myapp.db.mean
* myapp.db.last
* myapp.db.count
* myapp.db.sum

## API

* [Introduction](#introduction)
* [Endpoints](#endpoints)

### Introduction

In addition to sending data to librato, l2met also exposes the metrics via an HTTP API. This makes it possible to build your own dashboard and visualization tooling.

There are several things to consider when interacting with l2met's API.

* Bucket
* Metric

A **bucket** is identified by: token, name, time, resolution, and source. The bucket will hold the values for the identified metric. An example bucket looks like:

```
{
	"name": "some-function",
	"source": "myapp",
	"time": "1358486485",
	"resolution": "minute",
	"vals": [2.18, 3.14, 32, 64, 128.0]
}
```

A **metric** is identified by: token, name, time, resolution, and source. An example metric looks like:

```
{
	"name": "some-function",
	"source": "myapp",
	"time": "1358486485",
	"resolution": "minute",
	"min": 2.18,
	"median": 32,
	"perc95": 64,
	"perc99": 128.0,
	"max": 128.0,
	"mean": 45.86
}
```

The primary difference between a bucket and a metric is that the bucket provides all of the raw measurement data and the metric summarizes the data. For most cases, it will be more efficient to work with metrics. However, l2met exposes buckets for power users who want total control of the data.


### Endpoints

There are several read & write APIs. Systems can write logs using a syslog format. Other systems can read metrics and buckets using a json format. Authentication is required for both reading and writing. Furthermore, you can only read data that you have written. Authentication tokens are managed via [www.l2met.net](https://www.l2met.net).

#### Logs

If you are draining a Heroku app, you can simply add your l2met drain via the Heroku command line tool.

```bash
$ heroku drains:add https://l2met:your-token@drain.l2met.net/logs -a myapp
```

Or, you can use [log-shuttle](https://github.com/ryandotsmith/log-shuttle) to move your logs from $stdout to l2met.

```bash
$ export LOGPLEX_URL=https://l2met:your-token@drain.l2met.net/logs
$ echo 'measure=hello' | log-shuttle
```

Finally, you can just as easily post your logs directly to l2met.

```bash
$ curl "https://api.l2met.net/logs" \
	-X POST \
	-u 'l2met:your-l2met-token' \
	-d "63 <13>1 2000-01-01T00:44:30+00:00 token app web.1 - - measure=hello"
```

#### Metrics

There are several parameters to use when fetching metrics:

* limit
* group-by

Group-by determines the what time each metric should represent. For example, if a group-by=1 is used, each metric will represent 1 minute. If a group-by=60 is used, each metric will represent 60 minutes.

The limit parameter controls how how many grouped metrics are returned. For example, if a `group-by=15 limit=4` is used, then there will be 4 of each metrics represented by a 15 minute interval.

```bash
$ curl "https://api.l2met.net/metrics/your-metric?limit=1&offset=0&resolution=m" \
	-X GET \
	-u 'l2met:your-l2met-token'
```

#### Buckets

```bash
$ curl "https://api.l2met.net/buckets?limit=1&offset=0&resolution=m" \
	-X GET \
	-u 'l2met:your-l2met-token'
```

## Setup

If you are a Herokai, there is no need to setup this software. We have a production instance running and available for your use. Email ryan@heroku.com for instructions.

### Heroku Setup

You can run l2met in a multi-tenant mode or a single-user mode. The multi-tenant mode enable multiple drains with unique librato accounts. The single-user mode exposes 1 drain and maps to 1 librato account. The Heroku Setup assumes single-user mode.

#### Create Librato Account

Once you have created the account, visit your [settings](https://metrics.librato.com/account) page to grab you username and token. Keep this page open as you will need this data later in the setup.

#### Create a Heroku app.

```bash
$ git clone git://github.com/ryandotsmith/l2met.git
$ cd l2met
$ heroku create your-l2met --buildpack git://github.com/kr/heroku-buildpack-go.git
$ git push heroku master
```

#### Add database services.

I prefer redisgreen, however there are cheaper alternatives. Whichever provider you choose, ensure that you have set REDIS_URL properly.

```bash
$ heroku addons:add redisgreen:basic
$ heroku config:set REDIS_URL=$(heroku config -s | grep "^REDISGREEN_URL" | sed 's/REDISGREEN_URL=//')
```

#### Update Heroku config.

```bash
$ heroku config:set APP_NAME=your-l2met LOCAL_WORKERS=2
$ heroku config:set LIBRATO_USER=me@domain.com LIBRATO_TOKEN=abc123
```

#### Scale processes.

```bash
$ heroku scale web=1 librato_outlet=1
```

If you wish to run more than 1 outlet, you will need to adjust a config variable in addition to scaling the process. For example, if you are running 2 librato_outlets:

```bash
$ heroku config:set NUM_OUTLET_PARTITIONS=2
$ heroku scale librato_outlet=2
```

#### Add drain to your Heroku app(s)

Now that you have created an l2met app, you can drain logs from other heroku apps into l2met.

```bash
$ heroku drains:add https://l2met:`uuid`@your-l2met.herokuapp.com/logs -a your-app-that-needs-l2met
```

#### Test

Install [log-shuttle](https://github.com/ryandotsmith/log-shuttle).

```bash
$ curl -o log-shuttle "https://s3.amazonaws.com/32k.io/bin/log-shuttle"
$ chmod +x log-shuttle
```

Send data to l2met.

```bash
$ export LOGPLEX_URL=https://l2met:123@your-l2met.herokuapp.com/logs
$ echo 'measure="hello-from-your-l2met"' | log-shuttle
```

Now you should be able to see `hello-from-your-l2met` in your librato metrics web ui.

## Architecture

![img](http://f.cl.ly/items/3W2n313N3p1x0d0m1e35/l2met-arch.png)
