(ns pg-cljs.util
  "Pure helpers shared by pg-cljs: the explicit translation tables between
   kebab-case keywords and the names `pg` (node-postgres) spells, the
   validation of configuration maps and query commands, the structured
   error constructors of the one pg-cljs error taxonomy, and the copy-paste
   defense for values read from the environment.

   Pure: no I/O and no JavaScript interop. Turning the translated maps into
   JS objects, reading properties off JS errors and result objects is
   interop and lives in the single binding namespace (pg-cljs.binding);
   this namespace deals in strings, keywords, maps and vectors only. The
   one exception to the no-promises rule is run-op, the promise adapter
   every I/O namespace (core, pool, listen) wraps its upstream calls in:
   it is plumbing, not I/O.

   `pg` mixes camelCase (`connectionTimeoutMillis`, `rowCount`) and
   snake_case (`statement_timeout`, `client_encoding`) names, so every
   translation here goes through an explicit table rather than a generic
   kebab-to-camel rule. Column names and column values of query results are
   data, not wire codes: they are never translated by anything in this
   namespace."
  (:require [clojure.string :as str]
            [promesa.core :as p]))

;; ---------------------------------------------------------------------------
;; Translation tables: pg-cljs kebab keyword -> upstream name (config, query)
;; and upstream name -> pg-cljs keyword (result envelope, fields, errors,
;; notifications). The upstream spelling is the one the installed `pg` reads.
;; ---------------------------------------------------------------------------

(def client-config-keys
  "Configuration keys accepted by connect!, each with the upstream `pg`
   Client option it becomes. Millisecond options end in -ms; upstream reads
   them as milliseconds too.

   | key                                    | upstream                               |
   | :user :password :host :port :database  | same name                              |
   | :connection-string                     | connectionString                       |
   | :ssl                                   | ssl (boolean, string, or a map, see ssl-keys) |
   | :ssl-negotiation                       | sslnegotiation (\"postgres\" / \"direct\") |
   | :client-encoding                       | client_encoding                        |
   | :application-name                      | application_name                       |
   | :fallback-application-name             | fallback_application_name              |
   | :options                               | options (server command-line options)  |
   | :statement-timeout-ms                  | statement_timeout                      |
   | :lock-timeout-ms                       | lock_timeout                           |
   | :idle-in-transaction-session-timeout-ms | idle_in_transaction_session_timeout   |
   | :query-timeout-ms                      | query_timeout (client-side read timeout) |
   | :connection-timeout-ms                 | connectionTimeoutMillis (0 = none)     |
   | :keep-alive                            | keepAlive                              |
   | :keep-alive-initial-delay-ms           | keepAliveInitialDelayMillis            |
   | :types                                 | types (an object with getTypeParser)   |
   | :stream                                | stream (a Duplex or a function of the connection config returning one) |

   Values pass through untouched: password, user, database, connection
   string, stream factory. A key not in this table is a config-error before
   anything is sent."
  {:user "user"
   :password "password"
   :host "host"
   :port "port"
   :database "database"
   :connection-string "connectionString"
   :ssl "ssl"
   :ssl-negotiation "sslnegotiation"
   :client-encoding "client_encoding"
   :application-name "application_name"
   :fallback-application-name "fallback_application_name"
   :options "options"
   :statement-timeout-ms "statement_timeout"
   :lock-timeout-ms "lock_timeout"
   :idle-in-transaction-session-timeout-ms "idle_in_transaction_session_timeout"
   :query-timeout-ms "query_timeout"
   :connection-timeout-ms "connectionTimeoutMillis"
   :keep-alive "keepAlive"
   :keep-alive-initial-delay-ms "keepAliveInitialDelayMillis"
   :types "types"
   :stream "stream"})

(def pool-only-keys
  "Configuration keys accepted by pool! on top of client-config-keys, each
   with the upstream `pg.Pool` option it becomes. Every client key is a
   pool key too: the pool hands its whole options object to each client
   it creates.

   | key                   | upstream           | default upstream           |
   | :max                  | max                | 10 clients                 |
   | :min                  | min                | 0 (exempt from idle eviction; never pre-created) |
   | :max-uses             | maxUses            | Infinity (checkouts before a client is destroyed on release) |
   | :max-lifetime-seconds | maxLifetimeSeconds | 0 = off (age after which a client is destroyed at its next release, or at once when idle) |
   | :idle-timeout-ms      | idleTimeoutMillis  | 10000 (0 disables; applies to clients above :min) |
   | :allow-exit-on-idle   | allowExitOnIdle    | false (unref idle sockets and timers so Node can exit) |

   :connection-timeout-ms (a client key) also bounds the wait for a free
   client in a full pool. Not offered: onConnect / verify (JavaScript
   callback hooks run on the raw client), log, Client, Promise, poolSize."
  {:max "max"
   :min "min"
   :max-uses "maxUses"
   :max-lifetime-seconds "maxLifetimeSeconds"
   :idle-timeout-ms "idleTimeoutMillis"
   :allow-exit-on-idle "allowExitOnIdle"})

(def pool-config-keys
  "Every configuration key pool! accepts: client-config-keys plus
   pool-only-keys, with the upstream name each becomes."
  (merge client-config-keys pool-only-keys))

(def ssl-keys
  "Keys accepted inside an :ssl map and the Node `tls.connect` option each
   becomes. Values (certificate text, Buffers, functions) pass through."
  {:reject-unauthorized "rejectUnauthorized"
   :ca "ca"
   :key "key"
   :cert "cert"
   :servername "servername"
   :passphrase "passphrase"
   :ciphers "ciphers"
   :min-version "minVersion"
   :max-version "maxVersion"
   :check-server-identity "checkServerIdentity"})

(def query-keys
  "Keys of a query command map and the upstream query config field each
   becomes: :text (the SQL, $1..$n placeholders), :values (a vector of
   parameters), :name (prepared-statement name, parsed once per
   connection), :row-mode (:array for rows as vectors), :types (a per-query
   type-parser provider object), :query-timeout-ms (client-side read
   timeout for this query)."
  {:text "text"
   :values "values"
   :name "name"
   :row-mode "rowMode"
   :types "types"
   :query-timeout-ms "query_timeout"})

(def row-modes
  "Accepted :row-mode values. :array yields each row as a vector of values
   in column order; absent yields maps keyed by column name."
  #{:array})

(def result-keys
  "Upstream Result property -> result map key, in upstream order."
  [["command" :command]
   ["rowCount" :row-count]
   ["oid" :oid]])

(def field-keys
  "Upstream Field property -> field map key, in upstream order."
  [["name" :name]
   ["tableID" :table-id]
   ["columnID" :column-id]
   ["dataTypeID" :data-type-id]
   ["dataTypeSize" :data-type-size]
   ["dataTypeModifier" :data-type-modifier]
   ["format" :format]])

(def error-field-keys
  "Upstream DatabaseError / NoticeMessage property -> keyword, in upstream
   order. Every value is a string or absent; :code is the PostgreSQL
   SQLSTATE kept verbatim."
  [["severity" :severity]
   ["code" :code]
   ["detail" :detail]
   ["hint" :hint]
   ["position" :position]
   ["internalPosition" :internal-position]
   ["internalQuery" :internal-query]
   ["where" :where]
   ["schema" :schema]
   ["table" :table]
   ["column" :column]
   ["dataType" :data-type]
   ["constraint" :constraint]
   ["file" :file]
   ["line" :line]
   ["routine" :routine]])

(def notification-keys
  "Upstream NotificationResponseMessage property -> keyword."
  [["processId" :process-id]
   ["channel" :channel]
   ["payload" :payload]])

(def transaction-statuses
  "The ReadyForQuery status byte the server sends after every command (and
   `getTransactionStatus` returns) -> keyword: \"I\" idle (not in a
   transaction block), \"T\" inside a transaction block, \"E\" inside a
   FAILED transaction block (every command is rejected until ROLLBACK).
   Nothing else is ever sent; transaction-status maps an absent value to
   :unknown."
  {"I" :idle
   "T" :in-transaction
   "E" :in-failed-transaction})

(defn transaction-status
  "The keyword of transaction-statuses for a status string; :unknown for
   nil (no ReadyForQuery seen yet) or anything else."
  [s]
  (get transaction-statuses s :unknown))

;; ---------------------------------------------------------------------------
;; Errors: ONE taxonomy, :type under :pg-cljs/
;; ---------------------------------------------------------------------------

(def error-types
  "Every :type a pg-cljs ex-info can carry.
   - :pg-cljs/config-error — bad or unknown configuration, wrong argument
     shape; raised before anything is sent. Data: :message plus :arg,
     :value, :unknown, :allowed where applicable.
   - :pg-cljs/connect-error — the connection failed or is gone: refused,
     timed out, terminated, authentication rejected, used after end!.
     Data: :message :cause :op plus the upstream fields present (:code
     :errno :syscall :address :port for socket errors; the SQLSTATE fields
     for a server rejection).
   - :pg-cljs/query-error — the server rejected a query (every
     DatabaseError field keywordized, :code the SQLSTATE string) or the
     client-side query timeout elapsed. Data: :message :cause :op plus the
     fields present.
   - :pg-cljs/unsupported-error — the runtime is not Node.js."
  #{:pg-cljs/config-error :pg-cljs/connect-error :pg-cljs/query-error
    :pg-cljs/unsupported-error})

(defn- make-error [type message data]
  (ex-info (str "pg-cljs: " message)
           (merge {:type type :message message} data)))

(defn config-error
  "ex-info of :type :pg-cljs/config-error. `data` may carry :arg, :value,
   :unknown, :allowed, :op."
  [message & [data]]
  (make-error :pg-cljs/config-error message data))

(defn connect-error
  "ex-info of :type :pg-cljs/connect-error. `data` carries :cause, :op and
   the upstream fields present."
  [message & [data]]
  (make-error :pg-cljs/connect-error message data))

(defn query-error
  "ex-info of :type :pg-cljs/query-error. `data` carries :cause, :op and
   the DatabaseError fields present."
  [message & [data]]
  (make-error :pg-cljs/query-error message data))

(defn unsupported-error
  "ex-info of :type :pg-cljs/unsupported-error (not running on Node.js)."
  [message & [data]]
  (make-error :pg-cljs/unsupported-error message data))

(defn error-data
  "The structured data of a pg-cljs error, nil when `e` is not one."
  [e]
  (when-let [d (ex-data e)]
    (when (contains? error-types (:type d))
      d)))

(defn error-type
  "One of error-types, or nil when `e` is not a pg-cljs error."
  [e]
  (:type (error-data e)))

;; ---------------------------------------------------------------------------
;; Configuration
;; ---------------------------------------------------------------------------

(defn- config-of
  [op x]
  (cond
    (nil? x) {}
    (string? x) {:connection-string x}
    (map? x) x
    :else (throw (config-error (str (name op) "!: expected a config map, a connection string or nil")
                               {:arg :config :value x :op op}))))

(defn connect-config
  "The configuration map behind a connect! argument: nil -> {} (upstream
   then reads the PG* environment), a string -> {:connection-string s}, a
   map -> itself. Anything else is a config-error."
  [x]
  (config-of :connect x))

(defn pool-config
  "The configuration map behind a pool! argument: the same three forms as
   connect-config (nil, a connection string, a map); :op :pool in the
   config-error."
  [x]
  (config-of :pool x))

(defn normalize-config
  "Drops nil-valued entries of a config map. nil -> {}. Values are not
   touched otherwise."
  [config]
  (into {} (remove (comp nil? val)) config))

(defn- check-keys!
  [what allowed m data]
  (when-not (map? m)
    (throw (config-error (str what " must be a map") (assoc data :value m))))
  (let [unknown (remove #(contains? allowed %) (keys m))]
    (when (seq unknown)
      (throw (config-error (str what ": unknown key(s) " (pr-str (vec unknown))
                                "; allowed: " (pr-str (vec (sort (keys allowed)))))
                           (assoc data :unknown (set unknown) :allowed (set (keys allowed)))))))
  m)

(defn check-config!
  "Throws a config-error when `config` (already normalized) is not a map,
   has a key outside client-config-keys, or has an :ssl map with a key
   outside ssl-keys. Returns `config`. Upstream silently ignores unknown
   option names; pg-cljs fails before anything is sent."
  [config]
  (check-keys! "connect!: config" client-config-keys config {:arg :config :op :connect})
  (when (map? (:ssl config))
    (check-keys! "connect!: :ssl" ssl-keys (:ssl config) {:arg :ssl :op :connect}))
  config)

(defn check-pool-config!
  "The pool counterpart of check-config!: keys outside pool-config-keys
   (client keys plus pool-only-keys) and :ssl keys outside ssl-keys are a
   config-error with :op :pool. Returns `config`."
  [config]
  (check-keys! "pool!: config" pool-config-keys config {:arg :config :op :pool})
  (when (map? (:ssl config))
    (check-keys! "pool!: :ssl" ssl-keys (:ssl config) {:arg :ssl :op :pool}))
  config)

(defn- translate-keys
  "Kebab map -> map keyed by the upstream names of `table`."
  [table m]
  (into {} (map (fn [[k v]] [(get table k) v])) m))

(defn- upstream-config
  [table config]
  (let [m (translate-keys table config)]
    (if (map? (get m "ssl"))
      (assoc m "ssl" (translate-keys ssl-keys (get m "ssl")))
      m)))

(defn config->upstream
  "A validated kebab config map -> a map keyed by the upstream option
   names (strings). An :ssl map is translated through ssl-keys; every other
   value passes through verbatim."
  [config]
  (upstream-config client-config-keys config))

(defn pool-config->upstream
  "A validated pool config map -> a map keyed by the upstream `pg.Pool`
   option names (pool-config-keys); :ssl as in config->upstream."
  [config]
  (upstream-config pool-config-keys config))

;; ---------------------------------------------------------------------------
;; Query commands
;; ---------------------------------------------------------------------------

(defn- check-values!
  "Each parameter value must be something `pg` can prepare without
   guessing: strings, numbers, booleans, nil, vectors (PostgreSQL array
   literals) and JS values (Buffer, Date, bigint, objects with toPostgres).
   A ClojureScript map or set would be stringified by upstream into text
   that is neither JSON nor a PostgreSQL literal, so it is a config-error."
  [values]
  (when-not (sequential? values)
    (throw (config-error "query!: :values must be a vector"
                         {:arg :values :value values :op :query})))
  (doseq [[i v] (map-indexed vector values)]
    (cond
      (or (map? v) (set? v))
      (throw (config-error (str "query!: :values[" i "] is a ClojureScript collection pg cannot send; "
                                "pass JSON as a string and arrays as vectors")
                           {:arg :values :index i :value v :op :query}))
      (sequential? v) (check-values! v)))
  values)

(defn check-query!
  "Throws a config-error unless `cmd` is a map of query-keys with :text a
   string when present, at least one of :text / :name, :values a vector
   (see the parameter rules of the values check) when present and
   :row-mode one of row-modes when present. Returns `cmd`. A bare string
   is NOT accepted: a command is always a map."
  [cmd]
  (when-not (map? cmd)
    (throw (config-error "query!: the command must be a map {:text \"...\" :values [...]}"
                         {:arg :cmd :value cmd :op :query})))
  (check-keys! "query!: command" query-keys cmd {:arg :cmd :op :query})
  (when (and (contains? cmd :text) (not (string? (:text cmd))))
    (throw (config-error "query!: :text must be a string"
                         {:arg :text :value (:text cmd) :op :query})))
  (when-not (or (string? (:text cmd)) (string? (:name cmd)))
    (throw (config-error "query!: a command needs :text (or :name of an already prepared statement)"
                         {:arg :cmd :value cmd :op :query})))
  (when (and (contains? cmd :name) (not (string? (:name cmd))))
    (throw (config-error "query!: :name must be a string"
                         {:arg :name :value (:name cmd) :op :query})))
  (when (contains? cmd :values)
    (check-values! (:values cmd)))
  (when (and (contains? cmd :row-mode) (not (contains? row-modes (:row-mode cmd))))
    (throw (config-error (str "query!: :row-mode must be one of " (pr-str (vec row-modes)))
                         {:arg :row-mode :value (:row-mode cmd) :allowed row-modes :op :query})))
  cmd)

(defn query->upstream
  "A validated command map -> a map keyed by the upstream query config
   field names (strings); :row-mode :array becomes rowMode \"array\".
   :values stays the caller's vector (the binding turns it into a JS
   array); every other value passes through verbatim."
  [cmd]
  (let [m (translate-keys query-keys cmd)]
    (if (contains? m "rowMode")
      (assoc m "rowMode" (name (get m "rowMode")))
      m)))

;; ---------------------------------------------------------------------------
;; Failure classification for query!
;; ---------------------------------------------------------------------------

(def connection-gone-messages
  "The messages of the plain Errors `pg` rejects a query with when the
   connection is not there to run it (as opposed to the server refusing
   the query), including the pool's own: a pool used after end, a wait for
   a free client that exceeded :connection-timeout-ms, a new client that
   did not connect in time. These become :pg-cljs/connect-error."
  #{"Connection terminated"
    "Connection terminated unexpectedly"
    "Client has encountered a connection error and is not queryable"
    "Client was closed and is not queryable"
    "Cannot use a pool after calling end on the pool"
    "timeout exceeded when trying to connect"
    "Connection terminated due to connection timeout"})

(defn query-failure-kind
  "Given the properties read off an error that rejected a query
   (:database-error? :code :message), :connect when the connection is gone
   (a socket error with a :code, or one of connection-gone-messages) and
   :query otherwise (a DatabaseError, the query read timeout, a rejected
   command shape)."
  [{:keys [database-error? code message]}]
  (cond
    database-error? :query
    (contains? connection-gone-messages message) :connect
    (some? code) :connect
    :else :query))

;; ---------------------------------------------------------------------------
;; The promise adapter and the rejection normalizer shared by the I/O
;; namespaces. `info-of` is the binding's reader of a JS error's facts
;; (message, name, socket fields, DatabaseError fields); this namespace
;; never reads a JS object itself.
;; ---------------------------------------------------------------------------

(defn passthrough-error?
  "True when `e` already is a pg-cljs error or any other ex-info (caller
   code): those are rethrown untouched; only JS errors are normalized."
  [e]
  (or (some? (error-data e)) (instance? ExceptionInfo e)))

(defn normalize-failure
  "The ex-info for a rejection `e` of operation `op`: a passthrough error
   as it is; otherwise `kind` :connect -> a connect-error, :query -> a
   query-error unless query-failure-kind says the connection is gone (then
   a connect-error). `info-of` reads the error's facts
   (pg-cljs.binding/error-info); the data is those facts plus :cause (the
   JS error) and :op."
  [kind op info-of e]
  (if (passthrough-error? e)
    e
    (let [info (info-of e)
          data (assoc (dissoc info :database-error?) :cause e :op op)]
      (if (or (= :connect kind) (= :connect (query-failure-kind info)))
        (connect-error (:message info) data)
        (query-error (:message info) data)))))

(defn run-op
  "Runs `f` (which may throw synchronously and returns a promise or a
   value) and returns a promesa promise settled with its outcome, every
   rejection passed through `normalize`. Built with p/create around the
   upstream promise, never chained off a shared resolved promise."
  [normalize f]
  (p/create
   (fn [resolve reject]
     (try
       (let [r (f)]
         (if (p/promise? r)
           (p/handle r (fn [v e]
                         (if (some? e)
                           (reject (normalize e))
                           (resolve v))))
           (resolve r)))
       (catch :default e
         (reject (normalize e)))))))

;; ---------------------------------------------------------------------------
;; Environment values
;; ---------------------------------------------------------------------------

(defn unwrap-env-value
  "Copy-paste defense for a string read from the environment or a secrets
   file: trims whitespace and strips ONE layer of wrapping, either matching
   quotes (double, single or back quotes) or a leading and trailing dash.
   Nothing else is removed, so a password ending in punctuation survives.
   nil for a blank or non-string input. Not applied automatically: pg-cljs
   passes configuration values through verbatim and `pg` reads the PG*
   variables itself; this is for callers who assemble a config map from
   their own environment reads."
  [v]
  (let [v (when (string? v) (str/trim v))]
    (when-not (str/blank? v)
      (let [n (count v)
            wrapped? (fn [open close]
                       (and (> n 2) (str/starts-with? v open) (str/ends-with? v close)))
            v (if (or (wrapped? "\"" "\"") (wrapped? "'" "'") (wrapped? "`" "`") (wrapped? "-" "-"))
                (str/trim (subs v 1 (dec n)))
                v)]
        (when-not (str/blank? v) v)))))
