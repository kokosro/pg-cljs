# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- The client surface: `connect!` (config map / connection string / `(from-env)`),
  `query!` (command maps, parameterized values, `:row-mode :array`, named
  statements, multi-statement results), `end!`, `client?`, the error taxonomy
  (`:pg-cljs/config-error`, `:pg-cljs/connect-error`, `:pg-cljs/query-error`,
  `:pg-cljs/unsupported-error`), the explicit config-key translation table and
  the environment copy-paste defense in `pg-cljs.util`, the single JS-interop
  namespace `pg-cljs.binding`, and the wire-protocol fake PostgreSQL server of
  the offline suite.

## [0.1.0] - 2026-10-15

### Added

- Project skeleton: deps.edn, package.json (`pg` 8.23.1 pinned), standalone
  shadow-cljs.edn (`node-test`, `dev`, `examples`, `smoke` builds), the
  publicity gate wired first in `run-tests.sh`, `src/pg_cljs/core.cljs` with
  `api-version` `"0.1.0"`, dev and examples entry points, the cljs.test suite,
  and the `pg` (node-postgres) API inventory in `docs/pg-api-inventory.org`.

[0.1.0]: https://github.com/kokosro/pg-cljs/releases/tag/v0.1.0
