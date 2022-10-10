# `dobby` REST API

`dobby` has an option to spin up a REST service. To use it, simply pass a `--rest <port>` flag when starting up `dobbyd`.

Advantages of having a REST API:

- Flexible
- Scalable
- Cacheable
- Every call is a simple HTTP request

You can view `dobby`'s REST server implementation [here](../src/rest.rs).

`dobby` REST service is deployed on Heroku:
- http://dobby.lyova.xyz
- http://do88y.herokuapp.com

Try it out using `curl`:

```bash
# fetch the schema
$ curl http://dobby.lyova.xyz/.schema
{"tables":{"cars":{{"id":"int"},{"name":"string"},{"price":"float"}}},"name":"test-db","kind":"dobby"}

# insert some cars
$ curl -X POST -d '{"id":1,"name":"ferrari","price":123.456}' -H 'Content-Type: application/json' http://dobby.lyova.xyz/cars
$ curl -X POST -d '{"id":2,"name":"lambo","price":181.818}' -H 'Content-Type: application/json' http://dobby.lyova.xyz/cars

# select from cars the table
$ curl http://dobby.lyova.xyz/cars?id=1
[{"price":123.456,"id":1,"name":"ferrari"}]
```

> **hint**: use `jq` tool to pretty-print JSONs in the command line

## OpenAPI specification

Machine-readable OpenAPI spec is hosted on `/openapi.json`, derived from [this](../openapi.yaml) `.yaml` file.

Human-readable interactive documentation based on this spec is hosted on `/`: [check it out](http://dobby.lyova.xyz).

Writing an OpenAPI spec has a number of advantages:

- Code generation
- Tooling for documentation, tests and mocks
- Machine-readability
- Stability
