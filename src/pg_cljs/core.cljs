(ns pg-cljs.core
  "The pg-cljs client: construction of PostgreSQL clients from a kebab-case
   configuration map, a connection string or the PG* environment variables,
   and their lifecycle (connect, query, end). The public surface is plain
   ClojureScript data: configuration and query results are maps of
   kebab-cased keywords, asynchronous operations return promesa promises,
   and notifications arrive on core.async channels. No JS interop appears
   at a call site; the `pg` (node-postgres) objects stay inside the single
   binding namespace.

   Conventions:
   - names ending in `!` perform I/O and return a promise;
   - a client is an opaque handle (printing it shows host, port, database,
     user and status, never the password);
   - every rejection is an ex-info of ONE taxonomy (pg-cljs.util/error-types):
     :pg-cljs/config-error before anything is sent, :pg-cljs/connect-error
     when the connection fails or is gone, :pg-cljs/query-error when the
     server rejects a query or the query read timeout elapses,
     :pg-cljs/unsupported-error off Node.js; an ex-info thrown by caller
     code passes through untouched;
   - one query at a time per client: upstream queues a second concurrent
     query but deprecates doing so, so sequence queries (p/let) or use a
     pool;
   - nothing is chosen for the caller: no retries, no default timeouts
     (upstream's defaults apply: none), no type-parser override, no
     listener on the client's `error` event. An idle connection that
     breaks makes upstream emit `error`, and a Node EventEmitter with no
     `error` listener throws; subscribe with pg-cljs.binding/on when the
     process must survive an idle connection breaking."
  (:require [promesa.core :as p]
            [pg-cljs.binding :as b]
            [pg-cljs.util :as util]))

(def api-version
  "The library's own version marker, as a string. It names the version of
   the pg-cljs public surface, not the version of the wrapped `pg` package."
  "0.1.0")

;; ---------------------------------------------------------------------------
;; The handle
;; ---------------------------------------------------------------------------

(defrecord Client
  ;; js: the upstream pg.Client. state: atom {:status :connected | :ended}.
  [js state])

(extend-protocol IPrintWithWriter
  Client
  (-pr-writer [c w _]
    (-write w (str "#pg-cljs/client"
                   (pr-str (assoc (b/client-info (:js c)) :status (:status @(:state c))))))))

(defn client?
  "True for a client handle produced by connect!."
  [x]
  (instance? Client x))

(defn- check-client! [op c]
  (when-not (client? c)
    (throw (util/config-error (str (name op) "!: first argument must be a client handle")
                              {:arg :client :value c :op op}))))

;; ---------------------------------------------------------------------------
;; Error normalization and the one promise adapter
;; ---------------------------------------------------------------------------

(defn- passthrough?
  "pg-cljs errors and any other ex-info (caller code) pass through as they
   are; only JS errors are normalized."
  [e]
  (or (some? (util/error-data e)) (instance? ExceptionInfo e)))

(defn- ->connect-error
  [op e]
  (if (passthrough? e)
    e
    (let [info (b/error-info e)]
      (util/connect-error (:message info)
                          (assoc (dissoc info :database-error?) :cause e :op op)))))

(defn- ->query-failure
  "A rejection of query!: a server ErrorResponse and the read timeout are
   query-errors; a connection that is gone is a connect-error."
  [op e]
  (if (passthrough? e)
    e
    (let [info (b/error-info e)
          data (assoc (dissoc info :database-error?) :cause e :op op)]
      (if (= :connect (util/query-failure-kind info))
        (util/connect-error (:message info) data)
        (util/query-error (:message info) data)))))

(defn- run
  "Runs `f` (which may throw synchronously and returns a promise) and
   returns a promesa promise settled with its outcome, every rejection
   normalized through `normalize`."
  [normalize f]
  (p/create
   (fn [resolve reject]
     (try
       (p/handle (f)
                 (fn [v e]
                   (if (some? e)
                     (reject (normalize e))
                     (resolve v))))
       (catch :default e
         (reject (normalize e)))))))

;; ---------------------------------------------------------------------------
;; Construction
;; ---------------------------------------------------------------------------

(defn from-env
  "The configuration that makes `pg` read the connection settings from the
   environment: an empty map. With no value of its own for a setting, `pg`
   itself consults PGUSER, PGPASSWORD, PGHOST, PGPORT, PGDATABASE,
   PGSSLMODE, PGSSLNEGOTIATION, PGCLIENT_ENCODING, PGAPPNAME, PGOPTIONS,
   PGBINARY and PGREPLICATION (PGCONNECT_TIMEOUT is read but does not drive
   the JavaScript client's connection timer: set :connection-timeout-ms),
   then its own defaults (localhost, 5432, the OS user as user and
   database, no password, no SSL). Merge explicit keys over it to override
   a variable: (merge (from-env) {:application-name \"billing\"})."
  []
  {})

(defn connect!
  "Opens a connection and resolves with a client handle once the server is
   ready for queries. `config` is one of:
   - a map of pg-cljs.util/client-config-keys (:host :port :user :password
     :database :connection-string :ssl :application-name
     :connection-timeout-ms :query-timeout-ms :statement-timeout-ms ...);
     unknown keys are a config-error before anything is sent; a
     :connection-string's own parts override the explicit keys, as in `pg`;
   - a connection string (postgres://user:password@host:port/database?...);
   - nil or {} (see from-env): `pg` reads the PG* environment variables.
   Rejects with :pg-cljs/connect-error when the socket fails (:code
   \"ECONNREFUSED\" ...), the :connection-timeout-ms elapses (\"timeout
   expired\"), the socket closes during the handshake (\"Connection
   terminated unexpectedly\") or the server refuses the startup
   (authentication failure: the SQLSTATE fields are in the data). A client
   is single-use: once ended it cannot reconnect; call connect! again."
  [config]
  (run (partial ->connect-error :connect)
       (fn []
         (let [cfg (-> config util/connect-config util/normalize-config util/check-config!)
               js-client (b/make-client (b/config->js (util/config->upstream cfg)))
               handle (->Client js-client (atom {:status :connecting}))]
           (b/once js-client "end" (fn [] (swap! (:state handle) assoc :status :ended)))
           (p/then (b/connect js-client)
                   (fn [_]
                     (swap! (:state handle) assoc :status :connected)
                     handle))))))

;; ---------------------------------------------------------------------------
;; Queries
;; ---------------------------------------------------------------------------

(defn query!
  "Runs one command and resolves with its result. `cmd` is a map:
   {:text \"SELECT * FROM t WHERE id = $1\" :values [7]} with the optional
   :name (prepared statement, parsed once per connection and re-executed by
   name; the same name with a different text is rejected by upstream),
   :row-mode :array (rows as vectors in column order), :types (a per-query
   type-parser provider) and :query-timeout-ms (client-side read timeout:
   the query rejects with \"Query read timeout\" but the server keeps
   running it and the connection stays busy until it finishes). A string
   or any non-map command is a config-error; so is a missing :text,
   :values that is not a vector, or a ClojureScript map among the values.

   Parameter values are handed to `pg` as they are: strings, numbers,
   booleans and bigints are sent as their text, nil as NULL, vectors as
   PostgreSQL array literals, Buffers as bytea bytes, Dates as local-time
   ISO strings with offset.

   The result is {:rows [...] :fields [...] :command \"SELECT\" :row-count 2
   :oid nil}: :rows is a vector of maps keyed by the exact column-name
   strings the server sent (or of vectors with :row-mode :array), values
   exactly as `pg` yields them (int8 and numeric as strings, bytea as
   Buffer, timestamps as Date, json parsed); :fields is a vector of
   {:name :table-id :column-id :data-type-id :data-type-size
   :data-type-modifier :format}; :command is the first word of the
   command tag; :row-count the affected / returned count or nil (LOCK);
   :oid the inserted OID or nil. A multi-statement simple query (several
   statements in one :text, no :values) resolves with a VECTOR of such
   maps, one per statement.

   Rejects with :pg-cljs/query-error when the server rejects the command
   (data: :severity :code :detail :hint :position :schema :table :column
   :constraint ... as sent, :code the SQLSTATE string) or the read timeout
   elapses, and with :pg-cljs/connect-error when the connection is gone
   (\"Client was closed and is not queryable\" after end!, \"Connection
   terminated\", a socket error). A server rejection leaves the connection
   usable; inside a transaction it is then in the failed state."
  [client cmd]
  (run (partial ->query-failure :query)
       (fn []
         (check-client! :query client)
         (let [js-cmd (b/query->js (util/query->upstream (util/check-query! cmd)))]
           (p/then (b/query (:js client) js-cmd) b/result->data)))))

;; ---------------------------------------------------------------------------
;; Lifecycle
;; ---------------------------------------------------------------------------

(defn end!
  "Closes the connection and resolves with nil once the socket has closed.
   Upstream behavior, which pg-cljs keeps: with no query running, the
   Terminate message is sent and the socket closed gracefully; with a
   query running the socket is destroyed instead (that query and any
   queued ones reject with \"Connection terminated\", a connect-error, and
   no `error` event is emitted); on a connection that already broke the
   dead socket is destroyed. A second end! resolves at once: ending is
   idempotent. After end! every query! rejects with a connect-error
   (\"Client was closed and is not queryable\") and the client cannot be
   reconnected."
  [client]
  (run (partial ->connect-error :end)
       (fn []
         (check-client! :end client)
         (p/then (b/end (:js client))
                 (fn [_]
                   (swap! (:state client) assoc :status :ended)
                   nil)))))
