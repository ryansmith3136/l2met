# l2met

Turn these:
```ruby
$stdout.puts("measure#db.latency=4ms")
$stdout.puts("count#db.vaccum=1")
$stdout.puts("sample#db.size=100GB")
```

Into this:

![img](http://f.cl.ly/items/2R0h1x1b3V0Y0z0l1t1n/Screen%20Shot%202013-07-30%20at%209.59.52%20PM.png)


L2met receives HTTP requests that contain a body of RFC5424 formatted data. Commonly, data is drained into l2met by [logplex](https://github.com/heroku/logplex) or [log-shuttle](https://github.com/heroku/log-shuttle). Once data is delivered, l2met extracts and parses the log lines using the [logging conventions](https://github.com/ryandotsmith/l2met/wiki/Usage#log-conventions) and then stores the data in redis so that l2met outlets can read the data, build metrics, and deliver the metrics to your Librato account.

Checkout the wiki for information related to: [usage](https://github.com/ryandotsmith/l2met/wiki/Usage), [architecture](https://github.com/ryandotsmith/l2met/wiki/Architecture), and [administration](https://github.com/ryandotsmith/l2met/wiki/Administration).

## Getting Started

The easiest way to get l2met up and running is to deploy to Heroku. This guide assumes you have already created a Heroku & Librato account.

```bash
$ curl https://drone.io/github.com/ryandotsmith/l2met/files/l2met.tar.gz | tar xvz
$ ./setup my-l2met e@foo.com abc123
...
Drain URL: https://long-token@my-l2met.herokuapp.com/logs
```

This command will create Heroku app named *my-l2met* and return a drain URL with encrypted Librato credentials for a Librato account with email *e@foo.com* and an API token of *abc123*. After you have created *my-l2met*, you can add the drain URL to a Heroku app. A copy of the log stream will be delivered to *my-l2met* and metrics will be sent to the Librato account which your provided in the setup.

```bash
$ heroku drains:add https://long-token@my-l2met.herokuapp.com/logs -a myapp
```

## Hacking on l2met
Before working on a new feature, send your proposal to the [mailing list](https://groups.google.com/d/forum/l2met) for tips & feedback. Be sure to work on a feature branch and submit a PR when ready.

### Documentation
[GoDoc](http://godoc.org/github.com/ryandotsmith/l2met)

### Running Tests
[![Build Status](https://drone.io/github.com/ryandotsmith/l2met/status.png)](https://drone.io/github.com/ryandotsmith/l2met/latest)
```bash
$ ./redis-server --version
Redis server v=2.6.14 sha=f2f2b4eb:0 malloc=libc bits=64
$ go version
go version go1.1.1 darwin/amd64
```

```bash
$ git clone git://github.com/ryandotsmith/l2met.git
$ cd l2met
$ export SECRETS=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | openssl base64)
$ export TZ=UTC
$ export REDIS_URL=redis://localhost:6379
$ ./redis-server &
$ go test ./...
```
