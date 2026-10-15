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
taxonomy, features, settings, documentation and layout) as the surface is built; for
now the library exposes its version marker only.

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
