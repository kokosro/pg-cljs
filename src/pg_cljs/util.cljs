(ns pg-cljs.util
  "Pure helpers shared by pg-cljs: the explicit translation tables between
   kebab-case keywords and the names `pg` (node-postgres) spells, the
   validation of configuration maps and query commands, the structured
   error constructors of the one pg-cljs error taxonomy, and the copy-paste
   defense for values read from the environment.

   Pure: no I/O and no JavaScript interop. Turning the translated maps into
   JS objects, reading properties off JS errors and result objects is
   interop and lives in the single binding namespace (pg-cljs.binding);
   this namespace deals in strings, keywords, maps and vectors only.

   `pg` mixes camelCase (`connectionTimeoutMillis`, `rowCount`) and
   snake_case (`statement_timeout`, `client_encoding`) names, so every
   translation here goes through an explicit table rather than a generic
   kebab-to-camel rule. Column names and column values of query results are
   data, not wire codes: they are never translated by anything in this
   namespace."
  (:require [clojure.string :as str]))

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

(defn connect-config
  "The configuration map behind a connect! argument: nil -> {} (upstream
   then reads the PG* environment), a string -> {:connection-string s}, a
   map -> itself. Anything else is a config-error."
  [x]
  (cond
    (nil? x) {}
    (string? x) {:connection-string x}
    (map? x) x
    :else (throw (config-error "connect!: expected a config map, a connection string or nil"
                               {:arg :config :value x :op :connect}))))

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
                                "; allowed: " (pr-str (vec (sort allowed))))
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

(defn- translate-keys
  "Kebab map -> map keyed by the upstream names of `table`."
  [table m]
  (into {} (map (fn [[k v]] [(get table k) v])) m))

(defn config->upstream
  "A validated kebab config map -> a map keyed by the upstream option
   names (strings). An :ssl map is translated through ssl-keys; every other
   value passes through verbatim."
  [config]
  (let [m (translate-keys client-config-keys config)]
    (if (map? (get m "ssl"))
      (assoc m "ssl" (translate-keys ssl-keys (get m "ssl")))
      m)))

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
   the query). These become :pg-cljs/connect-error."
  #{"Connection terminated"
    "Connection terminated unexpectedly"
    "Client has encountered a connection error and is not queryable"
    "Client was closed and is not queryable"})

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
