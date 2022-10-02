# `dobby` gRPC API

`dobby` has an option to spin up a gRPC service. To use it, simply pass a `--grpc <port>` flag when starting up `dobbyd`.

gRPC has a number of advantages:

- Small binary payload (using protocol buffers)
- Strict specification (via the `.proto` file)
- Bi-directional streaming available
- Code generation for both servers and clients

Unfortunately, some shortcomings are present as well:

- No browser support
- Not humanly readable
- Restricted to HTTP/2 protocol

You can look up `dobby`'s the protocol specification in the [`.proto` file](../proto/database.proto).
You can also view `dobby`'s gRPC server [implementation](../src/grpc.rs).

Try it out using `dobby`'s [CLI client](./cli.md)!
