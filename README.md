# pg-cljs

A thin, faithful ClojureScript (Node.js) wrapper of
[node-postgres](https://node-postgres.com/) (the `pg` npm package): a PostgreSQL
client or pool is a configuration map, queries are text plus a vector of values, and
results come back as data. Requests and responses are maps of kebab-cased keywords,
asynchronous operations return [promesa](https://github.com/funcool/promesa) promises,
LISTEN/NOTIFY notifications are core.async channels, and the library has no runtime
npm dependency beyond `pg`. Column values are what `pg` yields and are never
reinterpreted.

This README grows into the full shape shared by the sibling libraries
(requirements, an offline quick start, going live, scenarios, pure helpers, the error
taxonomy, features, settings, documentation and layout) as the surface is built. The
client surface is in place: `connect!` (a config map of kebab-case keys, a
connection string, or `(from-env)` to let `pg` read the `PG*` variables), `query!`
(a command map `{:text "..." :values [...]}`, resolving `{:rows :fields :command
:row-count :oid}` with rows keyed by the exact column names), `end!` and
`transaction-status` (`:idle`, `:in-transaction`, `:in-failed-transaction`). Pools
live in `pg-cljs.pool`: `pool!` (the client keys plus `:max :min :max-uses
:max-lifetime-seconds :idle-timeout-ms :allow-exit-on-idle`), `connect!` (eager
first connection), `query!` (one command on a free client), `acquire!` / `release!`
/ `with-client!` (a dedicated client, the same handle as a `connect!` client, for
transactions and LISTEN), `status` (the live counters) and `end!`. LISTEN/NOTIFY
lives in `pg-cljs.listen`: `listen!` resolves with a core.async channel of
`{:channel :payload :process-id}` maps (sliding buffer of 1024 by default, the
`:buffer-size` option; the oldest notification is dropped when it is full),
`unlisten!` sends UNLISTEN and closes it, `notices!` is the same for server
notices. Every rejection is an `ex-info` whose `:type` is `:pg-cljs/config-error`,
`:pg-cljs/connect-error`, `:pg-cljs/query-error` or `:pg-cljs/unsupported-error`.

## Requirements

Node.js 22+, Java 11+ for shadow-cljs. The library is consumed as a source dependency:

```clojure
;; deps.edn of the consuming app, next to a checkout:
{:deps {io.github.kokosro/pg-cljs {:local/root "../pg-cljs"}}}
```

The npm side is the consuming app's: `pg` 8.23.1 in its `package.json`, resolved from
its `node_modules` at run time. `deps.edn` declares ClojureScript, core.async and
promesa.

```json
{"dependencies": {"pg": "8.23.1"}}
```

## Development

```sh
npm install
./run-tests.sh                 # publicity gate, then the offline cljs.test suite
npx shadow-cljs compile examples && node target/examples.js
```

## License

[Unlicense](LICENSE).
