# pg-cljs

A thin, faithful ClojureScript (Node.js) wrapper of
[node-postgres](https://node-postgres.com/) (the `pg` npm package): a PostgreSQL
client or pool is a configuration map, a query is text plus a vector of values, and
results, errors and notifications come back as data. Requests and responses are maps
of kebab-cased keywords, asynchronous operations return
[promesa](https://github.com/funcool/promesa) promises, LISTEN/NOTIFY notifications
and server notices are core.async channels, and the library has no runtime npm
dependency beyond `pg`. Column values are what `pg` yields and are never
reinterpreted: `int8` and `numeric` arrive as strings, `bytea` as a Buffer,
timestamps as a Date, `json` parsed. Everything the library does happens in the
process; only `connect!` and the pool open a socket, to the database the configuration
names.

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

## Quick start (offline)

The quick start connects, runs a parameterized SELECT and ends the connection, all
against a fake server: the real `pg` client runs the whole wire protocol (startup,
the extended query, Terminate) over a scripted socket that answers from a table, and
the library validates the configuration, builds the command and converts the result
exactly as it would for PostgreSQL. No server, no network, no password.

```bash
npm install
npx shadow-cljs compile examples && node target/examples.js
```

That runs [`dev/pg_cljs/examples.cljs`](dev/pg_cljs/examples.cljs), which holds every
example of this page and prints the results shown below. Its namespace form:

```clojure
(ns pg-cljs.examples
  (:require [cljs.core.async :as async]
            [cljs.pprint :as pprint]
            [promesa.core :as p]
            [pg-cljs.binding :as b]
            [pg-cljs.core :as pg]
            [pg-cljs.fake-server :as fake]
            [pg-cljs.listen :as listen]
            [pg-cljs.pool :as pool]
            [pg-cljs.util :as util]))
```

The configuration of a client is a map of the `pg` client options, kebab-cased:

```clojure
(def config
  "The connection settings of the examples: the keys of a `pg` client,
   kebab-cased. A real database adds :password, or leaves it to the
   PGPASSWORD variable, which `pg` reads for any key the map lacks."
  {:host             "db.example.test"
   :port             5432
   :user             "shop"
   :database         "shop"
   :application-name "pg-cljs-examples"})
```

A connection, one query, and the connection ended:

```clojure
(defn quick-start
  "Connects, reads the products cheaper than `max-cents`, ends the
   connection. Resolves with the rows and the envelope of the result."
  [config max-cents]
  (p/let [client (pg/connect! config)
          result (pg/query! client {:text "SELECT id, name, price_cents FROM products WHERE price_cents < $1 ORDER BY id"
                                    :values [max-cents]})
          _      (pg/end! client)]
    (select-keys result [:command :row-count :rows])))
```

`(quick-start config 500)` against the fake resolves with the first three entries;
`:sent` is the query the fake server received from the real client:

```clojure
{:command   "SELECT"
 :row-count 2
 :rows      [{"id" 1 "name" "pencil" "price_cents" "150"}
             {"id" 2 "name" "notebook" "price_cents" "450"}]
 :sent      {:text     "SELECT id, name, price_cents FROM products WHERE price_cents < $1 ORDER BY id"
             :values   ["500"]
             :protocol :extended}}
```

Rows are maps keyed by the exact column names the server sent, as strings. Values
are what `pg` yields: the `int4` column `id` is a number, the `int8` column
`price_cents` is a string, because a JavaScript number cannot hold every 64-bit
integer. The parameter went to the server as the text `"500"` through the extended
protocol; the server casts it. The full result also carries `:fields` (a vector of
`{:name :table-id :column-id :data-type-id :data-type-size :data-type-modifier
:format}`) and `:oid`.

### Going live

The same code against a database of yours. `from-env` gives the configuration that
lets `pg` read the standard `PG*` variables (`PGHOST`, `PGPORT`, `PGUSER`,
`PGPASSWORD`, `PGDATABASE`, and the rest of the table under
[Limits and settings](#limits-and-settings)); keys merged over it override the
variables. **This connects to your own database with your own credentials and runs
the statements you give it: whether and when to do that is your decision.**

```clojure
(defn live
  "The quick start against the database the PG* variables name (PGHOST,
   PGPORT, PGUSER, PGPASSWORD, PGDATABASE). Not run by `main`."
  []
  (quick-start (merge (pg/from-env) {:application-name      "pg-cljs-examples"
                                     :connection-timeout-ms 5000})
               500))
```

The library never reads a file: the credentials come to it in the configuration map
or through the environment that `pg` reads itself. Keep them in a `NAME=value` file
outside the repository that only you can read and load it into the process
environment with `set -a; . <secrets-file>; set +a`. A client handle prints its
host, port, database, user and status, never the password. Set
`:connection-timeout-ms`: without it a connection attempt to a host that does not
answer waits for the operating system, and `PGCONNECT_TIMEOUT` does not drive the
JavaScript client's timer.

## Scenarios

Each scenario is a function of `dev/pg_cljs/examples.cljs`, shown with the result it
resolves with against the fake. Given a configuration of yours, the same functions
run against PostgreSQL.

### A transaction on a pooled client

A pool is built from the client configuration plus the pool keys and connects
lazily. `with-client!` checks a client out for the duration of a function and
releases it whether the function resolves, rejects or throws; a transaction belongs
on such a dedicated client, never on `pool/query!`, whose successive calls may run
on different connections.

```clojure
(defn transfer-stock
  "Moves `qty` units of product `id` from the warehouse to the shop floor,
   in one transaction on a client checked out of `pool` for its duration.
   Resolves with the transaction status during and after, and the stock
   the second UPDATE returned."
  [pool id qty]
  (pool/with-client! pool
    (fn [client]
      (p/let [_      (pg/query! client {:text "BEGIN"})
              during (pg/transaction-status client)
              _      (pg/query! client {:text   "UPDATE stock SET warehouse = warehouse - $1 WHERE product_id = $2"
                                        :values [qty id]})
              moved  (pg/query! client {:text   "UPDATE stock SET floor = floor + $1 WHERE product_id = $2 RETURNING warehouse, floor"
                                        :values [qty id]})
              _      (pg/query! client {:text "COMMIT"})]
        {:during during
         :after  (pg/transaction-status client)
         :stock  (first (:rows moved))}))))
```

With `(pool/pool! (assoc config :max 2 :allow-exit-on-idle true))` connected eagerly
through `pool/connect!`, `(transfer-stock pool 1 3)`, then `pool/status` and
`pool/end!` give:

```clojure
{:transfer {:during :in-transaction :after :idle :stock {"warehouse" 7 "floor" 3}}
 :status   {:total-count 1 :idle-count 1 :waiting-count 0 :max 2}
 :ended    nil
 :sent     ["BEGIN"
            "UPDATE stock SET warehouse = warehouse - $1 WHERE product_id = $2"
            "UPDATE stock SET floor = floor + $1 WHERE product_id = $2 RETURNING warehouse, floor"
            "COMMIT"]}
```

`transaction-status` is synchronous and local: it reads the status byte the server
sent with the last ReadyForQuery, so it is `:in-transaction` after BEGIN and `:idle`
again after COMMIT. One client served the four statements, and sat idle in the pool
afterwards until `end!` ended it. A pooled client is the same handle as a `connect!`
client: `pg/query!`, `transaction-status` and `pg-cljs.listen` work on it unchanged.

### The transaction status across a failed block

A statement the server rejects inside a transaction block puts the block in the
failed state: every further command is rejected until ROLLBACK. The rejection itself
is a `:pg-cljs/query-error` with the SQLSTATE and every field the server sent.

```clojure
(defn transaction-states
  "BEGIN, a statement the server rejects, ROLLBACK: the transaction status
   the connection reports before, inside, after the failure and after the
   ROLLBACK, and the rejection that failed the block."
  [client]
  (p/let [before (pg/transaction-status client)
          _      (pg/query! client {:text "BEGIN"})
          open   (pg/transaction-status client)
          failed (-> (pg/query! client {:text "INSERT INTO products (id, name) VALUES (1, 'pencil')"})
                     (p/catch (fn [e] (select-keys (ex-data e) [:type :code :constraint]))))
          broken (pg/transaction-status client)
          _      (pg/query! client {:text "ROLLBACK"})]
    {:statuses [before open broken (pg/transaction-status client)]
     :failed   failed}))
```

On a connection to the fake, whose INSERT answers with a unique violation:

```clojure
{:statuses [:idle :in-transaction :in-failed-transaction :idle]
 :failed   {:type :pg-cljs/query-error :code "23505" :constraint "products_pkey"}}
```

### LISTEN / NOTIFY across two connections

`listen!` subscribes a connection to a channel name and resolves with a core.async
channel of notifications; a NOTIFY from any connection, here a second one, arrives
on it as a map. The upstream listener is attached before LISTEN is sent, so the
first notification after the subscription is never missed.

```clojure
(defn first-of
  "A promise of the first value taken from the core.async channel `ch`."
  [ch]
  (p/create (fn [resolve _] (async/take! ch resolve))))

(defn await-job
  "Subscribes `listener` to the \"jobs\" channel, has `worker` (another
   connection) announce a job, takes the notification and unsubscribes.
   Resolves with the notification and what the listener still listens to."
  [listener worker]
  (p/let [jobs (listen/listen! listener "jobs")
          _    (pg/query! worker {:text "SELECT pg_notify($1, $2)" :values ["jobs" "invoice-42"]})
          job  (first-of jobs)
          _    (listen/unlisten! listener "jobs")]
    {:job             job
     :still-listening (listen/listening listener)}))
```

With two connections to the fake, which delivers the NOTIFY of one to the other as
a server does, `(await-job listener worker)` resolves with the first two entries;
`:sent` is every query the server received, from both connections:

```clojure
{:job             {:process-id 4243 :channel "jobs" :payload "invoice-42"}
 :still-listening #{}
 :sent            ["LISTEN \"jobs\"" "SELECT pg_notify($1, $2)" "UNLISTEN \"jobs\""]}
```

The channel name is sent quoted as an identifier, so it is matched exactly and
case-sensitively. `:payload` is the NOTIFY payload verbatim (the empty string when
there was none), `:process-id` the backend process of the connection that sent it.
The channel has a sliding buffer of 1024 notifications (the `:buffer-size` option):
it fills whether or not anyone takes from it, and when it is full the oldest is
dropped; nothing is replayed. `unlisten!` closes the channel and sends UNLISTEN; the
channel also closes when the connection ends. A LISTEN belongs to one physical
connection: use a `connect!` client or a client checked out of a pool and not
released while listening. `notices!` gives the same kind of channel for the server's
RAISE NOTICE / WARNING messages.

## Pure helpers

No server, no network:

```clojure
(pg/from-env)
;; => {}

(pg/client? (pg/from-env))
;; => false

pg/api-version
;; => "0.1.0"

(util/transaction-status "T")
;; => :in-transaction

(sort (keys util/pool-only-keys))
;; => (:allow-exit-on-idle :idle-timeout-ms :max :max-lifetime-seconds :max-uses :min)

(util/config->upstream {:application-name "billing" :connection-timeout-ms 5000})
;; => {"application_name" "billing" "connectionTimeoutMillis" 5000}

(util/query->upstream {:text "SELECT $1" :values [1] :row-mode :array})
;; => {"text" "SELECT $1" "values" [1] "rowMode" "array"}

(util/unwrap-env-value "\"s3cret-\"")
;; => "s3cret-"

(sort util/error-types)
;; => (:pg-cljs/config-error :pg-cljs/connect-error :pg-cljs/query-error :pg-cljs/unsupported-error)

listen/default-buffer-size
;; => 1024
```

`from-env` is the empty configuration: with no value of its own for a setting, `pg`
consults the `PG*` variable, then its own default (localhost, 5432, the OS user as
user and database, no password, no SSL). `util` is `pg-cljs.util`: the translation
tables between the kebab-case keys and the names `pg` spells, applied by `connect!`,
`pool!` and `query!`. `unwrap-env-value` strips one layer of copy-paste wrapping
(quotes or dashes) from a value read from the environment and nothing else; the
library does not apply it itself.

## Error handling

Every rejection is an `ex-info` whose data carries a `:type`. A bad configuration,
a bad command and a wrong handle are refused before anything is sent:

| `:type`                        | when                                                                                                                             | data                                                                                                  |
|--------------------------------|----------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| `:pg-cljs/config-error`        | an unknown configuration or `:ssl` key, a command that is not a map, `:values` that is not a vector, a ClojureScript map among the values, a non-client or non-pool handle, a second `release!` | `:message`, `:op`, plus `:arg`, `:value`, `:unknown`, `:allowed`, `:index` as applicable |
| `:pg-cljs/connect-error`       | the socket failed or the connection is gone: refused, `:connection-timeout-ms` elapsed, terminated, authentication refused, a client used after `end!`, a pool used after `end!`, a wait for a free client that timed out | `:message`, `:cause` (the `pg` error), `:op`; `:code :errno :syscall :address :port` for a socket error; the SQLSTATE fields for a refused startup |
| `:pg-cljs/query-error`         | the server rejected a query (an ErrorResponse), or `:query-timeout-ms` elapsed                                                    | `:message`, `:cause`, `:op`; `:code` the SQLSTATE string and `:severity :detail :hint :position :schema :table :column :constraint ...` as the server sent them |
| `:pg-cljs/unsupported-error`   | the runtime is not Node.js                                                                                                        | `:platform`                                                                                           |

A server rejection leaves the connection usable (inside a transaction block it is
then in the failed state); a `connect-error` means the handle is finished: connect
again. An `ex-info` thrown by caller code inside `with-client!` passes through
untouched.

```clojure
(defn outcome
  "The value of `(f)`, or its failure as data: the :type of every pg-cljs
   rejection and the fields that matter for it. A server rejection carries
   the SQLSTATE in :code and every field the server sent."
  [f]
  (-> (p/let [value (f)] {:ok? true :value value})
      (p/catch (fn [e]
                 (let [{:keys [type] :as data} (ex-data e)]
                   (case type
                     :pg-cljs/query-error   {:ok? false :why :query
                                             :error (select-keys data [:code :severity :message :detail :hint :position :table :constraint])}
                     :pg-cljs/connect-error {:ok? false :why :connection
                                             :error (select-keys data [:message :code])}
                     :pg-cljs/config-error  {:ok? false :why :configuration
                                             :error (select-keys data [:arg :unknown])}
                     (throw e)))))))
```

For a SELECT from a misspelled table, an INSERT that violates a unique constraint, a
command given as a string instead of a map, a configuration with a key the client
does not have, and a query after `end!`, `outcome` resolves with:

```clojure
{:missing-table {:ok? false :why :query
                 :error {:code "42P01" :severity "ERROR"
                         :message "relation \"prodcuts\" does not exist" :position "15"}}
 :duplicate     {:ok? false :why :query
                 :error {:code "23505" :severity "ERROR"
                         :message "duplicate key value violates unique constraint \"products_pkey\""
                         :detail "Key (id)=(1) already exists."
                         :table "products" :constraint "products_pkey"}}
 :not-a-map     {:ok? false :why :configuration :error {:arg :cmd}}
 :unknown-key   {:ok? false :why :configuration :error {:arg :config :unknown #{:timeout}}}
 :after-end     {:ok? false :why :connection :error {:message "Client was closed and is not queryable"}}}
```

`p/let` evaluates `(f)` inside a promise, so what the library throws synchronously
(a bad command, an unknown key) arrives at the same `p/catch` as what it rejects
with. The `:message` of a config-error names the allowed keys; the `:code` of a
connect-error is the socket error code (`"ECONNREFUSED"`) when there is one.

## Features

* **Clients** — `connect!` from a kebab-case map, a connection string or the `PG*`
  environment; `query!` with `:text`, `:values`, a prepared-statement `:name`,
  `:row-mode :array` and a per-query `:query-timeout-ms`; `end!`;
  `transaction-status` read locally.
* **Results as data** — `{:rows :fields :command :row-count :oid}`, rows keyed by the
  exact column names, a vector of such maps for a multi-statement simple query.
* **Pools** — `pool!` with `:max :min :max-uses :max-lifetime-seconds
  :idle-timeout-ms :allow-exit-on-idle`; `connect!` for an eager first connection;
  `query!` on a free client; `acquire!` / `release!` / `with-client!` for a dedicated
  one; `status` with the live counters; `end!`. Aliases `pool-connect!`,
  `pool-query!`, `pool-end!`, `pool-status` for code that refers both namespaces.
* **LISTEN/NOTIFY as channels** — `listen!` gives a core.async channel per channel
  name with a sliding buffer; `unlisten!` closes it; `listening` lists the open
  names; `notices!` does the same for server notices.
* **Readable keys, untouched values** — configuration keys and query options are
  translated through explicit tables to the names `pg` spells; column values and
  parameter values pass through as they are.
* **Structured errors** — every rejection is an `ex-info` with a `:type` of one
  taxonomy; server rejections carry the SQLSTATE and every field sent.
* **Validation up front** — an unknown configuration key, a malformed command or a
  wrong handle is refused before anything is sent; `pg` itself would ignore an
  unknown option silently.
* **Nothing decided for the caller** — no retries, no default timeouts, no type-parser
  override, no listener on the `error` event of a client or pool.
* **No interop at call sites** — the public namespaces carry no `^js` hints;
  JavaScript lives in one binding namespace, `pg-cljs.binding`.

## Limits and settings

The limits that matter are PostgreSQL's and `pg`'s: the size of a NOTIFY payload,
the number of connections the server allows, the statement and lock timeouts the
server enforces. The library enforces none of them and invents none: it reports what
the server or `pg` refused. It adds no retries, no reconnection and no timeouts of
its own; every timeout below is a `pg` option with `pg`'s default, and absent means
none.

Client configuration (`connect!`; every key is a pool key too):

| key                                        | default                        | meaning                                                                     |
|--------------------------------------------|--------------------------------|-----------------------------------------------------------------------------|
| `:host`                                    | `PGHOST`, then `localhost`     | the server; a path starting with `/` is a Unix socket directory             |
| `:port`                                    | `PGPORT`, then 5432            |                                                                             |
| `:user`                                    | `PGUSER`, then the OS user     |                                                                             |
| `:password`                                | `PGPASSWORD`, then none        | a string; never printed by a handle                                         |
| `:database`                                | `PGDATABASE`, then the user    |                                                                             |
| `:connection-string`                       | none                           | `postgres://user:password@host:port/database?...`; its parts override the keys above |
| `:ssl`                                     | `PGSSLMODE`, then off          | `true`, `false`, or a map of `:reject-unauthorized :ca :key :cert :servername :passphrase :ciphers :min-version :max-version :check-server-identity` |
| `:ssl-negotiation`                         | `PGSSLNEGOTIATION`, then `"postgres"` | `"direct"` starts TLS at once (PostgreSQL 17+)                        |
| `:client-encoding`                         | `PGCLIENT_ENCODING`, then UTF8 |                                                                             |
| `:application-name`                        | `PGAPPNAME`, then none         | shown in `pg_stat_activity`                                                 |
| `:fallback-application-name`               | none                           | used when `:application-name` is absent                                     |
| `:options`                                 | `PGOPTIONS`, then none         | server command-line options                                                 |
| `:statement-timeout-ms`                    | none                           | server-side; sent as a startup parameter                                    |
| `:lock-timeout-ms`                         | none                           | server-side; sent as a startup parameter                                    |
| `:idle-in-transaction-session-timeout-ms`  | none                           | server-side; sent as a startup parameter                                    |
| `:query-timeout-ms`                        | none                           | client-side read timeout: the query rejects, the server keeps running it    |
| `:connection-timeout-ms`                   | none                           | how long `connect!` waits for the server; in a pool, also how long `acquire!` waits for a free client |
| `:keep-alive`, `:keep-alive-initial-delay-ms` | off, 0                      | TCP keep-alive                                                              |
| `:types`                                   | `pg`'s parsers                 | a type-parser provider object with `getTypeParser`                          |
| `:stream`                                  | a TCP socket                   | a Duplex stream, or a function of the connection config returning one       |

Pool configuration (`pool!`, on top of the client keys):

| key                      | default   | meaning                                                                    |
|--------------------------|-----------|----------------------------------------------------------------------------|
| `:max`                   | 10        | clients at most                                                            |
| `:min`                   | 0         | clients exempt from idle eviction; never created ahead of need             |
| `:max-uses`              | unlimited | checkouts after which a client is ended at its release                     |
| `:max-lifetime-seconds`  | 0 (off)   | age after which a client is ended at its release, or at once when idle     |
| `:idle-timeout-ms`       | 10000     | how long an idle client above `:min` is kept; 0 keeps it forever           |
| `:allow-exit-on-idle`    | `false`   | let the Node process exit while idle clients are kept; otherwise `end!` the pool |

Channels (`listen!`, `notices!`):

| option          | default | meaning                                                              |
|-----------------|---------|----------------------------------------------------------------------|
| `:buffer-size`  | 1024    | notifications held, sliding: when full, the oldest is dropped        |

The environment variables `pg` reads for a key the configuration lacks:

| variable             | key                  | note                                                                  |
|----------------------|----------------------|-----------------------------------------------------------------------|
| `PGUSER`             | `:user`              |                                                                       |
| `PGPASSWORD`         | `:password`          |                                                                       |
| `PGHOST`             | `:host`              |                                                                       |
| `PGPORT`             | `:port`              |                                                                       |
| `PGDATABASE`         | `:database`          |                                                                       |
| `PGSSLMODE`          | `:ssl`               | `disable` off; `prefer`, `require`, `verify-ca`, `verify-full` on with full verification; `no-verify` on without |
| `PGSSLNEGOTIATION`   | `:ssl-negotiation`   |                                                                       |
| `PGCLIENT_ENCODING`  | `:client-encoding`   |                                                                       |
| `PGAPPNAME`          | `:application-name`  |                                                                       |
| `PGOPTIONS`          | `:options`           |                                                                       |
| `PGBINARY`           | binary results       | not offered as a key; the string `"true"` counts as on                |
| `PGREPLICATION`      | replication mode     | not offered as a key                                                  |
| `PGCONNECT_TIMEOUT`  | none                 | read by `pg` but not honored by the JavaScript client: set `:connection-timeout-ms` |

What the library does not do: it does not retry or reconnect; it does not override a
type parser (`int8` and `numeric` stay strings, `numeric[]` arrives as floating-point
numbers, as `pg` parses it); it attaches no listener to the `error` event of a client
or pool, so an idle connection that breaks makes Node throw unless the process
subscribes with `(pg-cljs.binding/on (:js client) "error" f)`; it runs one query at a
time per client, so sequence queries with `p/let` or use a pool.

## Documentation

* [`docs/pg-cljs.org`](docs/pg-cljs.org) — the manual: the client handle, one query
  at a time, promises and channels, the configuration translation, the client, pool
  and listen guides, the gotchas observed live, testing and the live smoke, and the
  reference of every public function.
* [`docs/pg-api-inventory.org`](docs/pg-api-inventory.org) — the surface of `pg` that
  the library wraps: the Client and Pool options, the environment variables, the
  Result and error shapes, the type parsers, the connection string, LISTEN/NOTIFY,
  with the source each fact was taken from and the open questions.
* The docstrings of `src/pg_cljs/` are the reference of each function.

## Layout

```
src/pg_cljs/
  core.cljs            from-env, connect!, query!, transaction-status, end!, client?, api-version
  pool.cljs            pool!, connect!, query!, acquire!, release!, with-client!, status, end!
  listen.cljs          listen!, unlisten!, listening, notices!
  util.cljs            the translation tables, validation, the error taxonomy, the promise adapter
  binding.cljs         the one JavaScript namespace: the pg module, Client, Pool, events, errors
dev/pg_cljs/
  examples.cljs        the examples of this page, offline
  smoke.cljs           a live check against a PostgreSQL server, run by hand
  dev.cljs             the Node runtime a REPL attaches to
test/pg_cljs/
  fake_server.cljs     the wire-protocol fake server the suite and the examples run against
docs/
  pg-cljs.org              the manual
  pg-api-inventory.org     the API inventory
```

## Public API index

* `pg-cljs.core`: `(from-env)`, `(connect! config)`, `(query! client cmd)`,
  `(transaction-status client)`, `(end! client)`, `(client? x)`, `api-version`.
* `pg-cljs.pool`: `(pool! config)`, `(connect! pool)`, `(query! pool cmd)`,
  `(acquire! pool)`, `(release! client)`, `(release! client destroy?)`,
  `(with-client! pool f)`, `(status pool)`, `(end! pool)`, `(pool? x)`, and the
  aliases `pool-connect!`, `pool-query!`, `pool-end!`, `pool-status`.
* `pg-cljs.listen`: `(listen! client channel-name)`, `(listen! client channel-name
  opts)`, `(unlisten! client channel-name)`, `(listening client)`, `(notices! client)`,
  `(notices! client opts)`, `default-buffer-size`, `listen-options`.
* `pg-cljs.util`: the tables `client-config-keys`, `pool-only-keys`,
  `pool-config-keys`, `ssl-keys`, `query-keys`, `error-types`; `(transaction-status
  s)`, `(config->upstream config)`, `(query->upstream cmd)`, `(unwrap-env-value v)`,
  `(error-type e)`, `(error-data e)`, and the validators `connect!`, `pool!` and
  `query!` run.
* `pg-cljs.binding` (the JavaScript namespace, for those who hold a `pg` object):
  `(on emitter event f)`, `(once emitter event f)`, `(off emitter event f)`,
  `(pg-version)`, `(escape-identifier s)`. The documented path never needs them.

## Development

```bash
npm install
./run-tests.sh     # the publicity gate, then the unit suite; no network
npx shadow-cljs compile examples && node target/examples.js   # the examples of this page; no network
npx shadow-cljs watch dev                                     # a Node runtime for the REPL
```

The suite runs the real `pg` client and pool against the wire-protocol fake server:
configuration translation and validation, connect, the simple and extended query
protocols, results, errors, termination, pools and their counters, LISTEN/NOTIFY
and notices, and the examples of this page: it fails when this page shows code or a
result that the examples do not produce, and when the manual's API reference and
the public functions of the source disagree. The one real socket the suite opens
goes to 127.0.0.1:9 to prove a refused connection is reported. `./run-tests.sh`
ends by replacing its shell with node, so nothing can follow it on the same command
line.

The live check connects to a PostgreSQL server of yours and is run by hand, with the
connection settings in the environment (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`,
`PGDATABASE`), or in a `NAME=value` file named by `PG_CLJS_SECRETS_FILE`:

```bash
npx shadow-cljs compile smoke && node target/smoke.js
```

It creates temporary tables in its own session, runs every capability of the library
against them (queries, a transaction and a rollback, a prepared statement, a pool,
LISTEN/NOTIFY over a second connection, a server rejection, a query timeout), drops
what it created, prints one `ok` or `FAIL` line per check and a summary, and exits 0
when every check passed. It changes nothing that existed before it ran. Read
[`dev/pg_cljs/smoke.cljs`](dev/pg_cljs/smoke.cljs) before running it.

## License

[Unlicense](LICENSE).
