# `dobby`

![ci](https://github.com/ly0va/dobby/actions/workflows/ci.yml/badge.svg)
[![heroku](https://heroku-badge.herokuapp.com/?app=do88y)](http://dobby.lyova.xyz)
[![dependency status](https://deps.rs/repo/github/ly0va/dobby/status.svg)](https://deps.rs/repo/github/ly0va/dobby)

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
- :rocket: Try `dobby` on [Heroku](http://dobby.lyova.xyz)!
- :computer: Cross-platform!
- :zap: Blazingly fast!

## Details

You can read about each module in the [docs](./docs):

- [Architechture](./docs/architecture.md)
- [Server](./docs/server.md)
- [CLI client](./docs/cli.md)
- [REST service](./docs/rest-api.md)
- [gRPC service](./docs/grpc-api.md)

## Installation

### Native installation

> **note**: this guide is only applicable for Linux/MacOS users. If you use Windows... why?

**Build dependencies**: Rust 1.64+, Protobuf 3+

```
# rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# protoc (ubuntu/debian)
apt install -y protobuf-compiler

# protoc (arch)
pacman -Ss protobuf

# protoc (macOS)
brew install protobuf
```

Installing `dobby`:

```
cargo install --bins --git https://github.com/ly0va/dobby
```

This provides two binaries, `dobbyd` (the daemon) and `dobby` (the client)

### Docker image

```
docker pull ghcr.io/ly0va/dobby:master
```
