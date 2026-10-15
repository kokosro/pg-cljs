(ns pg-cljs.binding
  "The single JavaScript-interop namespace of pg-cljs: it requires `pg`
   (node-postgres) lazily from the project's node_modules, constructs
   Clients, invokes connect / query / end, subscribes to Client events,
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

(defn client-info
  "The non-secret connection facts of a Client: {:host :port :database
   :user} (the password is not read)."
  [client]
  (props->map [["host" :host] ["port" :port] ["database" :database] ["user" :user]]
              client))

;; ---------------------------------------------------------------------------
;; Events
;; ---------------------------------------------------------------------------

(defn on
  "`client.on(event, f)`; returns f (the listener to remove later)."
  [client event f]
  (js-invoke client "on" event f)
  f)

(defn once
  "`client.once(event, f)`; returns f."
  [client event f]
  (js-invoke client "once" event f)
  f)

(defn off
  "`client.removeListener(event, f)`; returns nil."
  [client event f]
  (js-invoke client "removeListener" event f)
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
