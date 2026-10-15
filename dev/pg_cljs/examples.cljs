(ns pg-cljs.examples
  "The examples of the README, runnable OFFLINE:

     npx shadow-cljs compile examples && node target/examples.js

   The library does all it would do for a PostgreSQL server: it validates
   the configuration, constructs the real `pg` Client or Pool, which runs
   the whole wire protocol (startup, simple and extended queries,
   LISTEN, Terminate), and converts every result, error and notification
   into data. What it talks to is a fake: a scripted socket
   (pg-cljs.fake-server), handed to `pg` through its `stream` option,
   that answers each query from a table with the backend messages a
   server would send. No PostgreSQL server, no network, no password.

   What stands between a README:begin and a README:end mark stands in
   README.md as written (test/pg_cljs/examples_test.cljs holds the
   README to it and runs every function of this file)."
  (:require [cljs.core.async :as async]
            [cljs.pprint :as pprint]
            [promesa.core :as p]
            [pg-cljs.binding :as b]
            [pg-cljs.core :as pg]
            [pg-cljs.fake-server :as fake]
            [pg-cljs.listen :as listen]
            [pg-cljs.pool :as pool]
            [pg-cljs.util :as util]))

;; ---------------------------------------------------------------------------
;; Quick start: the configuration
;; ---------------------------------------------------------------------------

;; README:begin config
(def config
  "The connection settings of the examples: the keys of a `pg` client,
   kebab-cased. A real database adds :password, or leaves it to the
   PGPASSWORD variable, which `pg` reads for any key the map lacks."
  {:host             "db.example.test"
   :port             5432
   :user             "shop"
   :database         "shop"
   :application-name "pg-cljs-examples"})
;; README:end config

;; ---------------------------------------------------------------------------
;; The fake server: what it answers, by query text
;; ---------------------------------------------------------------------------

(def ^:private products-fields
  [{:name "id" :data-type-id 23}
   {:name "name" :data-type-id 25}
   {:name "price_cents" :data-type-id 20}])

(def ^:private duplicate-key
  {:error {:severity "ERROR" :code "23505"
           :message "duplicate key value violates unique constraint \"products_pkey\""
           :detail "Key (id)=(1) already exists."
           :schema "public" :table "products" :constraint "products_pkey"}})

(def script
  "What the fake server answers, by the text of the query: the fields and
   rows as wire text (the real `pg` parses them: int4 to numbers, int8 to
   strings), the command tag, the transaction status byte, and the
   notifications a NOTIFY delivers."
  {"SELECT id, name, price_cents FROM products WHERE price_cents < $1 ORDER BY id"
   {:fields products-fields
    :rows [["1" "pencil" "150"] ["2" "notebook" "450"]]
    :command "SELECT 2"}

   "BEGIN" {:command "BEGIN" :status "T"}
   "COMMIT" {:command "COMMIT" :status "I"}
   "ROLLBACK" {:command "ROLLBACK" :status "I"}

   "UPDATE stock SET warehouse = warehouse - $1 WHERE product_id = $2"
   {:command "UPDATE 1" :status "T"}

   "UPDATE stock SET floor = floor + $1 WHERE product_id = $2 RETURNING warehouse, floor"
   {:fields [{:name "warehouse" :data-type-id 23} {:name "floor" :data-type-id 23}]
    :rows [["7" "3"]]
    :command "UPDATE 1"
    :status "T"}

   "INSERT INTO products (id, name) VALUES (1, 'pencil')"
   (assoc duplicate-key :status "E")

   "INSERT INTO products (id, name, price_cents) VALUES ($1, $2, $3)"
   duplicate-key

   "LISTEN \"jobs\"" {:command "LISTEN"}
   "UNLISTEN \"jobs\"" {:command "UNLISTEN"}
   "SELECT pg_notify($1, $2)"
   {:fields [{:name "pg_notify" :data-type-id 2278}]
    :rows [[""]]
    :command "SELECT 1"
    :notify-all [{:process-id 4243 :channel "jobs" :payload "invoice-42"}]}

   "SELECT * FROM prodcuts"
   {:error {:severity "ERROR" :code "42P01"
            :message "relation \"prodcuts\" does not exist"
            :position "15"}}})

(defn fake-server
  "A fake server answering from `script`."
  []
  (fake/fake-server {:script script :process-id 4242}))

(defn offline
  "`config` pointed at `server`: the `pg` Client opens the fake socket of
   the server instead of a TCP connection. Pure data: the :stream key is
   a `pg` client option like any other."
  [server config]
  (assoc config :stream (fake/stream-option server)))

(defn no-server!
  "Installs a tripwire where the library turns a configuration into the
   options of a `pg` Client or Pool: from here on, a configuration without
   a :stream fake is refused. An example that forgot its fake fails at
   once instead of opening a socket."
  []
  (let [real b/config->js]
    (set! b/config->js
          (fn [upstream-config]
            (when-not (contains? upstream-config "stream")
              (throw (ex-info "offline: no connection is opened here; the config needs a :stream fake"
                              {:type :pg-cljs.examples/offline})))
            (real upstream-config)))))

;; ---------------------------------------------------------------------------
;; Quick start
;; ---------------------------------------------------------------------------

;; README:begin quick-start
(defn quick-start
  "Connects, reads the products cheaper than `max-cents`, ends the
   connection. Resolves with the rows and the envelope of the result."
  [config max-cents]
  (p/let [client (pg/connect! config)
          result (pg/query! client {:text "SELECT id, name, price_cents FROM products WHERE price_cents < $1 ORDER BY id"
                                    :values [max-cents]})
          _      (pg/end! client)]
    (select-keys result [:command :row-count :rows])))
;; README:end quick-start

(defn quick-start-offline
  "`quick-start` against the fake server, with what the server received."
  []
  (let [server (fake-server)]
    (p/let [result (quick-start (offline server config) 500)]
      (assoc result :sent (select-keys (first (fake/queries server)) [:text :values :protocol])))))

;; README:begin going-live
(defn live
  "The quick start against the database the PG* variables name (PGHOST,
   PGPORT, PGUSER, PGPASSWORD, PGDATABASE). Not run by `main`."
  []
  (quick-start (merge (pg/from-env) {:application-name      "pg-cljs-examples"
                                     :connection-timeout-ms 5000})
               500))
;; README:end going-live

;; ---------------------------------------------------------------------------
;; Scenario: a transaction on a pooled client
;; ---------------------------------------------------------------------------

;; README:begin pool
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
;; README:end pool

(defn pool-offline
  "A pool of two against the fake server: connected eagerly, `transfer-stock`
   run on it, its counters read, then ended. Resolves with each step."
  []
  (let [server (fake-server)
        pool   (pool/pool! (offline server (assoc config :max 2 :allow-exit-on-idle true)))]
    (p/let [_        (pool/connect! pool)
            moved    (transfer-stock pool 1 3)
            status   (pool/status pool)
            ended    (pool/end! pool)]
      {:transfer moved
       :status   (select-keys status [:total-count :idle-count :waiting-count :max])
       :ended    ended
       :sent     (mapv :text (fake/queries server))})))

;; ---------------------------------------------------------------------------
;; Scenario: the transaction status across a failed block
;; ---------------------------------------------------------------------------

;; README:begin transaction-states
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
;; README:end transaction-states

(defn transaction-states-offline
  "`transaction-states` on a connection to the fake server."
  []
  (let [server (fake-server)]
    (p/let [client (pg/connect! (offline server config))
            states (transaction-states client)
            _      (pg/end! client)]
      states)))

;; ---------------------------------------------------------------------------
;; Scenario: LISTEN / NOTIFY across two connections
;; ---------------------------------------------------------------------------

;; README:begin listen
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
;; README:end listen

(defn listen-offline
  "`await-job` with two connections to the fake server, which delivers the
   NOTIFY of one to the other as a server does."
  []
  (let [server (fake-server)]
    (p/let [listener (pg/connect! (offline server config))
            worker   (pg/connect! (offline server config))
            got      (await-job listener worker)
            _        (pg/end! listener)
            _        (pg/end! worker)]
      (assoc got :sent (mapv :text (fake/queries server))))))

;; ---------------------------------------------------------------------------
;; Error handling
;; ---------------------------------------------------------------------------

;; README:begin errors
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
;; README:end errors

(defn errors-offline
  "`outcome` of a query on a misspelled table, of an INSERT that violates
   a unique constraint, of a command that is not a map, of a configuration
   with a key the client does not have, and of a query after end!."
  []
  (let [server (fake-server)]
    (p/let [client        (pg/connect! (offline server config))
            missing-table (outcome #(pg/query! client {:text "SELECT * FROM prodcuts"}))
            duplicate     (outcome #(pg/query! client {:text   "INSERT INTO products (id, name, price_cents) VALUES ($1, $2, $3)"
                                                       :values [1 "pencil" 150]}))
            not-a-map     (outcome #(pg/query! client "SELECT 1"))
            unknown-key   (outcome #(pg/connect! (offline server (assoc config :timeout 5))))
            _             (pg/end! client)
            after-end     (outcome #(pg/query! client {:text "SELECT 1"}))]
      {:missing-table missing-table
       :duplicate     duplicate
       :not-a-map     not-a-map
       :unknown-key   unknown-key
       :after-end     after-end})))

;; ---------------------------------------------------------------------------
;; Runner
;; ---------------------------------------------------------------------------

(defn- show [title value]
  (println (str ";; " title))
  (pprint/pprint value))

(defn main [& _]
  (no-server!)
  (-> (p/let [started  (quick-start-offline)
              moved    (pool-offline)
              states   (transaction-states-offline)
              notified (listen-offline)
              failed   (errors-offline)]
        (show "quick start" started)
        (show "a transaction on a pooled client" moved)
        (show "the transaction status across a failed block" states)
        (show "LISTEN / NOTIFY across two connections" notified)
        (show "failures are data" failed)
        (println ";; pure helpers")
        (println "(pg/from-env) ;; =>" (pr-str (pg/from-env)))
        (println "pg/api-version ;; =>" (pr-str pg/api-version))
        (println "(util/transaction-status \"T\") ;; =>" (pr-str (util/transaction-status "T"))))
      (p/catch (fn [e]
                 (println "example failed:" (ex-message e) (pr-str (ex-data e)))
                 (set! (.-exitCode js/process) 1)))))
