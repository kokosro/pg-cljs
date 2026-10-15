(ns pg-cljs.binding
  "The single JavaScript-interop namespace of pg-cljs: it requires `pg`
   (node-postgres) lazily from the project's node_modules, constructs
   Clients and Pools, invokes connect / query / end / release, reads the
   pool counters, subscribes to Client and Pool events,
   reads properties off upstream errors, and converts the fixed-shape
   upstream envelopes (configuration keys, the Result object, Field
   metadata, DatabaseError fields, notification and notice messages) to
   ClojureScript data through the tables of pg-cljs.util.

   What it never converts: column values (what `pg` yields is what the
   caller gets: strings for int8 and numeric, Buffers for bytea, Dates for
   timestamps, parsed JSON for json), query parameter values (handed to
   `pg` as they are), the password, the connection string, the stream
   factory. Column names stay the exact strings the server sent.

   Every function returns upstream's own value (native promises, JS
   errors); pg-cljs.core turns them into promesa promises and ex-info
   errors."
  (:require [pg-cljs.util :as util]))

;; ---------------------------------------------------------------------------
;; Module loading
;; ---------------------------------------------------------------------------

(defn node?
  "True when the runtime is Node.js (process.versions.node exists)."
  []
  (and (exists? js/process)
       (exists? js/process.versions)
       (exists? js/process.versions.node)))

(defn- resolve-project-module
  "Absolute path of a module under the current working directory's
   node_modules, so `pg` resolves from the consuming project even when the
   compiled script lives elsewhere."
  [module]
  (let [path (js/require "path")]
    (.resolve path (js/process.cwd) "node_modules" module)))

(defonce ^:private pg-cache (atom nil))

(defn pg-module
  "The `pg` module object, required once from the project's node_modules.
   Throws :pg-cljs/unsupported-error outside Node.js."
  []
  (when-not (node?)
    (throw (util/unsupported-error "pg-cljs needs Node.js: `pg` is a Node package"
                                   {:platform "browser"})))
  (or @pg-cache
      (reset! pg-cache (js/require (resolve-project-module "pg")))))

(defn pg-version
  "The installed `pg` package version string, nil outside Node.js."
  []
  (when (node?)
    (unchecked-get (js/require (resolve-project-module "pg/package.json")) "version")))

;; ---------------------------------------------------------------------------
;; Property access
;; ---------------------------------------------------------------------------

(defn- prop
  "Property `k` of JS object `o`; nil for nil / undefined / absent."
  [o k]
  (when (some? o)
    (let [v (unchecked-get o k)]
      (when-not (undefined? v) v))))

(defn- props->map
  "Reads the [upstream-name keyword] pairs of `table` off `o`, keeping the
   present ones."
  [table o]
  (persistent!
   (reduce (fn [m [js-name k]]
             (let [v (prop o js-name)]
               (if (some? v) (assoc! m k v) m)))
           (transient {})
           table)))

;; ---------------------------------------------------------------------------
;; CLJS -> JS (configuration, query command)
;; ---------------------------------------------------------------------------

(defn- values->js
  "A vector of query parameters -> a JS array; nested vectors become nested
   arrays (PostgreSQL array literals); every other element is untouched."
  [values]
  (let [arr (array)]
    (doseq [v values]
      (.push arr (if (sequential? v) (values->js v) v)))
    arr))

(defn- plain->js
  "A string-keyed map -> a JS object; nested string-keyed maps are
   converted the same way; every other value is set as it is."
  [m]
  (let [o (js-obj)]
    (doseq [[k v] m]
      (unchecked-set o k (if (map? v) (plain->js v) v)))
    o))

(defn config->js
  "The upstream-keyed config map of pg-cljs.util/config->upstream -> the
   options object handed to `new Client`."
  [upstream-config]
  (plain->js upstream-config))

(defn query->js
  "The upstream-keyed command map of pg-cljs.util/query->upstream -> the
   query config object handed to `client.query`; \"values\" becomes a JS
   array."
  [upstream-cmd]
  (let [o (plain->js (dissoc upstream-cmd "values"))]
    (when (contains? upstream-cmd "values")
      (unchecked-set o "values" (values->js (get upstream-cmd "values"))))
    o))

;; ---------------------------------------------------------------------------
;; Client construction and invocation (upstream values returned as they are)
;; ---------------------------------------------------------------------------

(defn make-client
  "`new pg.Client(options)`. Never connects. Throws upstream's synchronous
   errors (an invalid sslnegotiation value)."
  [js-options]
  (let [ctor (unchecked-get (pg-module) "Client")]
    (new ctor js-options)))

(defn connect
  "`client.connect()`: the native promise, resolving with the client."
  [client]
  (js-invoke client "connect"))

(defn end
  "`client.end()`: the native promise, resolving with undefined once the
   socket closed."
  [client]
  (js-invoke client "end"))

(defn query
  "`client.query(config)`: the native promise of the Result (or an array of
   Results for a multi-statement simple query)."
  [client js-cmd]
  (js-invoke client "query" js-cmd))

(defn transaction-status
  "`client.getTransactionStatus()`: \"I\", \"T\", \"E\" or nil before the
   first ReadyForQuery."
  [client]
  (let [s (js-invoke client "getTransactionStatus")]
    (when (some? s) s)))

(def ^:private connection-facts
  [["host" :host] ["port" :port] ["database" :database] ["user" :user]])

(defn client-info
  "The non-secret connection facts of a Client: {:host :port :database
   :user} (the password is not read)."
  [client]
  (props->map connection-facts client))

(defn escape-identifier
  "`pg.escapeIdentifier(s)`: the string double-quoted as a SQL identifier
   with inner double quotes doubled, so it names exactly that channel or
   relation, case preserved."
  [s]
  (js-invoke (pg-module) "escapeIdentifier" s))

;; ---------------------------------------------------------------------------
;; Pool construction and invocation
;; ---------------------------------------------------------------------------

(defn make-pool
  "`new pg.Pool(options)`. Never connects; the whole options object is
   handed to every Client the pool creates."
  [js-options]
  (let [ctor (unchecked-get (pg-module) "Pool")]
    (new ctor js-options)))

(defn pool-connect
  "`pool.connect()`: the native promise of a checked-out Client that
   carries its own `release` function."
  [pool]
  (js-invoke pool "connect"))

(defn pool-query
  "`pool.query(config)`: the native promise of the Result; the pool checks
   a client out, runs the one query and releases it (with the error when
   the query failed, which removes that client from the pool)."
  [pool js-cmd]
  (js-invoke pool "query" js-cmd))

(defn pool-end
  "`pool.end()`: the native promise resolving with undefined once every
   client of the pool has been told to end."
  [pool]
  (js-invoke pool "end"))

(defn pool-release
  "`client.release()` of a checked-out pooled Client, or
   `client.release(v)` when `v` is non-nil (a truthy value: the client is
   ended and removed instead of returned to the idle set). Synchronous;
   returns nil. Upstream THROWS a plain Error on a second release of the
   same checkout."
  [client v]
  (if (some? v)
    (js-invoke client "release" v)
    (js-invoke client "release"))
  nil)

(defn pool-counts
  "The live counters of a Pool as data: {:total-count :idle-count
   :waiting-count :expired-count :ending? :ended?} (totalCount, idleCount,
   waitingCount, expiredCount, ending, ended)."
  [pool]
  {:total-count (prop pool "totalCount")
   :idle-count (prop pool "idleCount")
   :waiting-count (prop pool "waitingCount")
   :expired-count (prop pool "expiredCount")
   :ending? (boolean (prop pool "ending"))
   :ended? (boolean (prop pool "ended"))})

(defn pool-info
  "The non-secret facts of a Pool's resolved options: {:host :port
   :database :user :max :min :max-uses :max-lifetime-seconds
   :idle-timeout-ms :allow-exit-on-idle} for the ones present (the
   password is not read; upstream also hides it from enumeration)."
  [pool]
  (props->map (into connection-facts
                    [["max" :max] ["min" :min] ["maxUses" :max-uses]
                     ["maxLifetimeSeconds" :max-lifetime-seconds]
                     ["idleTimeoutMillis" :idle-timeout-ms]
                     ["allowExitOnIdle" :allow-exit-on-idle]])
              (prop pool "options")))

;; ---------------------------------------------------------------------------
;; Events
;; ---------------------------------------------------------------------------

(defn on
  "`emitter.on(event, f)` on a Client or a Pool; returns f (the listener to
   remove later)."
  [emitter event f]
  (js-invoke emitter "on" event f)
  f)

(defn once
  "`emitter.once(event, f)`; returns f."
  [emitter event f]
  (js-invoke emitter "once" event f)
  f)

(defn off
  "`emitter.removeListener(event, f)`; returns nil. Works for a listener
   attached with once too."
  [emitter event f]
  (js-invoke emitter "removeListener" event f)
  nil)

;; ---------------------------------------------------------------------------
;; Errors
;; ---------------------------------------------------------------------------

(defn database-error?
  "True when `e` is a `pg.DatabaseError` (a server ErrorResponse)."
  [e]
  (let [ctor (prop (pg-module) "DatabaseError")]
    (and (some? ctor) (instance? ctor e))))

(defn error-info
  "The facts of a JS error, as data: :message, :name, the socket fields
   (:code :errno :syscall :address :port) when present, every DatabaseError
   field present (keywordized, :code the SQLSTATE string) and
   :database-error? true for a server rejection."
  [e]
  (let [db? (database-error? e)
        base (cond-> {:message (or (prop e "message") (str e))}
               (string? (prop e "name")) (assoc :name (prop e "name"))
               db? (assoc :database-error? true))
        fields (if db?
                 (props->map util/error-field-keys e)
                 (props->map [["code" :code] ["errno" :errno] ["syscall" :syscall]
                              ["address" :address] ["port" :port]]
                             e))]
    (merge base fields)))

;; ---------------------------------------------------------------------------
;; JS -> CLJS (result envelope, fields, rows, notifications, notices)
;; ---------------------------------------------------------------------------

(defn- field->data [f]
  (props->map util/field-keys f))

(defn- row->data
  "A row object -> a map keyed by the exact column-name strings; a row
   array (rowMode array) -> a vector. Values untouched."
  [row]
  (if (array? row)
    (vec row)
    (persistent!
     (reduce (fn [m k] (assoc! m k (unchecked-get row k)))
             (transient {})
             (js/Object.keys row)))))

(defn- one-result->data
  "Every key of util/result-keys is present (nil when upstream has null),
   plus :rows and :fields."
  [r]
  (-> (into {} (map (fn [[js-name k]] [k (prop r js-name)])) util/result-keys)
      (assoc :rows (mapv row->data (or (prop r "rows") (array)))
             :fields (mapv field->data (or (prop r "fields") (array))))))

(defn result->data
  "A Result -> {:command :row-count :oid :rows :fields}; an array of Results
   (multi-statement simple query) -> a vector of such maps."
  [r]
  (if (array? r)
    (mapv one-result->data r)
    (one-result->data r)))

(defn notification->data
  "A NotificationResponseMessage -> {:process-id :channel :payload}."
  [msg]
  (props->map util/notification-keys msg))

(defn notice->data
  "A NoticeMessage -> {:message plus the severity / code / detail / ...
   fields present} (same keys as a query-error)."
  [msg]
  (merge {:message (prop msg "message")}
         (props->map util/error-field-keys msg)))
