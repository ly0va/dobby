# `dobby`

![ci](https://github.com/ly0va/dobby/actions/workflows/ci.yml/badge.svg)
[![dependency status](https://deps.rs/repo/github/ly0va/dobby/status.svg)](https://deps.rs/repo/github/ly0va/dobby)

<img align="right" width="125" height="125" src="./docs/img/dobby.svg">

*A database engine as poor as a house elf*

> **disclaimer**: this is a university project. Please don't jump to any conclusions when you see
> poorly designed, poorly tested, feature-deprived, underdocumented and/or buggy modules.
> This is not meant for public use. For educational purposes only.

## About

`dobby` is a homemade table-oriented (but not really relational) database engine with a modular design.

## Features

- :floppy_disk: Basic CRUD operations
- :gear: Filtering based on column values
- :hammer_and_wrench: Creating and dropping tables
- :pencil: Renaming columns
- :envelope: A modern REST API
- :package: An even more modern gRPC API
- :sparkles: A fancy CLI client
- :ledger: Logging
- :feather: Optional [SQLite](sqlite.org) back-end
- :rocket: Try `dobby` on [Heroku](http://dobby.lyova.xyz)!
- :computer: Cross-platform!
- :zap: Blazingly fast!

## Details

You can read about each module in the docs:

- [Architechture](./docs/architecture.md)
- [Server](./docs/server.md)
- [CLI client](./docs/cli.md)
- [REST service](./docs/rest-api.md)
- [gRPC service](./docs/grpc-api.md)
- [SQLite mode](./docs/sqlite.md)
- [Testing](./docs/testing.md)

## Installation

### Native installation

> **note**: this guide is only applicable for Linux/MacOS users. If you use Windows... why?

**Build dependencies**: Rust 1.56+, Protobuf 3+

```bash
# rust
$ curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# protoc (ubuntu/debian)
$ apt install -y protobuf-compiler

# protoc (arch)
$ pacman -S protobuf

# protoc (macOS)
$ brew install protobuf
```

Installing `dobby` via cargo:

```bash
cargo install --bins --git https://github.com/ly0va/dobby
```

Or building from source:

```bash
$ git clone https://github.com/ly0va/dobby
$ cd dobby
$ cargo build
```

This provides two binaries, `dobbyd` (the daemon) and `dobby` (the client)

### Docker image

```bash
docker pull ghcr.io/ly0va/dobby:master
```

## Credits

- Logo created by [David S](https://thenounproject.com/david.o.s.16/) from The Noun Project
- Everything else in this repo - by yours truly
