# `sqlite` backend

If you, for some reason, want to use `dobby`'s APIs with an `sqlite` back-end, it is possible with the `--sqlite` server flag.

SQLite's advantages:

- Better performance
- Portability
- Reliability
- Accessiblity

Module implementation: [here](../src/core/database/sqlite.rs).

All inputs are properly sanitized, so no [SQL-injections](https://www.w3schools.com/sql/sql_injection.asp) are possible.

> **note**: obviously, a very small subset of SQLite's features is supported
