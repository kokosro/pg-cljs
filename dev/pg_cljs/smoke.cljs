(ns pg-cljs.smoke
  "The LIVE smoke: every capability of the library against a REAL
   PostgreSQL server that the person running it owns. Run by hand, never
   by run-tests.sh:

     npx shadow-cljs compile smoke && node target/smoke.js

   Connection settings: the PG* variables of the environment (PGHOST,
   PGPORT, PGUSER, PGPASSWORD, PGDATABASE; PGSSLMODE when the server wants
   TLS), which `pg` reads itself through (pg/from-env). Alternatively,
   PG_CLJS_SECRETS_FILE names a NAME=value file (one per line, # comments)
   whose PG* entries are put into the environment of this process for the
   keys the environment does not already set; the environment wins. No
   file is read unless that variable is set, and no value of either is
   ever printed: a handle prints host, port, database and user only.

     set -a; . <secrets-file>; set +a; node target/smoke.js
     PG_CLJS_SECRETS_FILE=<secrets-file> node target/smoke.js

   What it does to the database: it creates ONE temporary table
   (pg_cljs_smoke, session-scoped, dropped before the session ends and
   gone with it in any case), inserts into it, runs a transaction and a
   rollback on it, LISTENs on the channel pg_cljs_smoke and NOTIFYs it,
   raises one NOTICE, selects from a table that does not exist, and runs
   pg_sleep(1) once to show the query read timeout. Nothing that existed
   before it ran is read, changed or dropped. It needs a role that may
   create temporary tables and LISTEN; nothing more.

   Structure: numbered checks that never abort early (a failure prints
   `FAIL <name>: <reason>` and the next check runs; only a failed connect
   skips the rest), each printing one line `ok <name> -- <evidence>` or
   `FAIL ...`, then the cleanup, the summary line `<n> ok, <m> failed`
   and the exit code (0 when nothing failed). The process must exit on
   its own after the summary: every client and pool is ended, so a process
   that stays alive means a leaked socket or timer, and is itself a
   finding.

   What it settles that the offline suite cannot: a query read timeout
   leaves the server running the statement (the next query on that
   connection waits until pg_sleep finishes), a NOTIFY without payload
   arrives as the empty string, int8 arrives as a string past 2^53, bytea
   as bytes, and the SQLSTATE of a missing relation is 42P01."
  (:require [cljs.core.async :as async]
            [clojure.string :as str]
            [promesa.core :as p]
            [pg-cljs.core :as pg]
            [pg-cljs.listen :as listen]
            [pg-cljs.pool :as pool]
            [pg-cljs.util :as util]))

;; ---------------------------------------------------------------------------
;; The environment and the optional secrets file (dev harness plumbing: the
;; library itself never reads a file or the environment)
;; ---------------------------------------------------------------------------

(def secrets-file-variable "PG_CLJS_SECRETS_FILE")

(defn- env [k]
  (let [v (unchecked-get js/process.env k)]
    (when (string? v) v)))

(defn- load-secrets-file!
  "Puts the PG* entries of the NAME=value file at `path` into the process
   environment, for the names not already set. Values go through
   util/unwrap-env-value (one layer of quotes or dashes stripped)."
  [path]
  (let [fs (js/require "fs")
        text (.readFileSync fs path "utf8")
        lines (str/split-lines text)]
    (doseq [line lines
            :let [line (str/trim line)]
            :when (and (seq line) (not (str/starts-with? line "#")))
            :let [[k v] (str/split line #"=" 2)
                  k (str/trim (str/replace k #"^export\s+" ""))]
            :when (and (str/starts-with? k "PG") (some? v) (nil? (env k)))]
      (when-let [v (util/unwrap-env-value v)]
        (unchecked-set js/process.env k v)))
    (println (str "secrets file loaded: " path))))

(def config
  "The connection settings: whatever PG* names, plus an application name
   and a bounded connection wait."
  (merge (pg/from-env) {:application-name "pg-cljs-smoke"
                        :connection-timeout-ms 10000}))

;; ---------------------------------------------------------------------------
;; Bookkeeping
;; ---------------------------------------------------------------------------

(def results (atom []))

(defn- say [& parts] (println (str/join " " parts)))

(defn- record! [name ok? evidence]
  (swap! results conj {:name name :ok? ok?})
  (say (if ok? "ok  " "FAIL") name "--" evidence))

(defn check!
  "Runs `(f)`, a promise of {:ok? bool :evidence str} or of a plain value
   (truthy = ok), and records one line. A rejection is a FAIL with its
   message and data, never a crash."
  [name f]
  (-> (p/let [r (f)]
        (if (and (map? r) (contains? r :ok?))
          (record! name (boolean (:ok? r)) (str (:evidence r)))
          (record! name (boolean r) (pr-str r))))
      (p/catch (fn [e]
                 (record! name false (str (ex-message e) " " (pr-str (dissoc (ex-data e) :cause))))))))

(defn- expect
  "{:ok? :evidence} of `actual` against `expected`."
  [what expected actual]
  {:ok? (= expected actual)
   :evidence (if (= expected actual)
               (str what " = " (pr-str actual))
               (str what ": expected " (pr-str expected) ", got " (pr-str actual)))})

(defn- failure-data
  "Promise of the ex-data of the rejection of `pr`, or {:resolved v}."
  [pr]
  (-> pr
      (p/then (fn [v] {:resolved v}))
      (p/catch (fn [e] (dissoc (ex-data e) :cause)))))

(defn- take-or-timeout
  "A promise of the next value of `ch`, or :timeout after `ms`."
  [ch ms]
  (p/race [(p/create (fn [resolve _] (async/take! ch resolve)))
           (p/delay ms :timeout)]))

(def table "pg_cljs_smoke")
(def channel "pg_cljs_smoke")
(def big "9007199254740993")

;; ---------------------------------------------------------------------------
;; The checks, in order, on one main connection
;; ---------------------------------------------------------------------------

(defn- check-connect! [client]
  (check! "connect"
          (fn []
            (p/let [r (pg/query! client {:text "SELECT version() AS v, current_user AS u"})]
              {:ok? (and (pg/client? client) (= :idle (pg/transaction-status client)))
               :evidence (str (first (str/split (get (first (:rows r)) "v") #" on ")) ", status "
                              (pg/transaction-status client))}))))

(defn- check-temp-table! [client]
  (check! "create temp table"
          (fn []
            (p/let [r (pg/query! client {:text (str "CREATE TEMP TABLE " table
                                                    " (id int4 PRIMARY KEY, big int8, blob bytea, note text)")})]
              (expect "command" "CREATE" (:command r))))))

(defn- check-roundtrip! [client]
  (check! "insert / select roundtrip: int8 as string, bytea as bytes"
          (fn []
            (p/let [ins (pg/query! client {:text (str "INSERT INTO " table " (id, big, blob, note) VALUES ($1, $2, $3::bytea, $4)")
                                           :values [1 big "\\x0001ff" "ok"]})
                    sel (pg/query! client {:text (str "SELECT id, big, blob, encode(blob, 'hex') AS hex, note FROM " table " WHERE id = $1")
                                           :values [1]})
                    row (first (:rows sel))
                    blob-field (some #(when (= "blob" (:name %)) %) (:fields sel))]
              {:ok? (and (= 1 (:row-count ins))
                         (= 1 (get row "id"))
                         (= big (get row "big"))
                         (string? (get row "big"))
                         (= "0001ff" (get row "hex"))
                         (not (string? (get row "blob")))
                         (= 17 (:data-type-id blob-field)))
               :evidence (str "big " (pr-str (get row "big")) " (a string), blob oid "
                              (:data-type-id blob-field) " hex " (get row "hex"))}))))

(defn- check-parameterized! [client]
  (check! "parameterized query, maps and :row-mode :array"
          (fn []
            (p/let [m (pg/query! client {:text (str "SELECT note, big FROM " table " WHERE id = $1") :values [1]})
                    a (pg/query! client {:text (str "SELECT note, big FROM " table " WHERE id = $1") :values [1]
                                         :row-mode :array})]
              (expect "rows" [[{"note" "ok" "big" big}] [["ok" big]]] [(:rows m) (:rows a)])))))

(defn- check-transaction! [client]
  (check! "BEGIN / COMMIT with transaction-status"
          (fn []
            (p/let [s0 (pg/transaction-status client)
                    _ (pg/query! client {:text "BEGIN"})
                    s1 (pg/transaction-status client)
                    _ (pg/query! client {:text (str "UPDATE " table " SET note = $1 WHERE id = $2") :values ["committed" 1]})
                    _ (pg/query! client {:text "COMMIT"})
                    s2 (pg/transaction-status client)
                    r (pg/query! client {:text (str "SELECT note FROM " table " WHERE id = 1")})]
              (expect "statuses and note" [[:idle :in-transaction :idle] "committed"]
                      [[s0 s1 s2] (get (first (:rows r)) "note")])))))

(defn- check-rollback! [client]
  (check! "BEGIN / ROLLBACK undoes the insert"
          (fn []
            (p/let [_ (pg/query! client {:text "BEGIN"})
                    _ (pg/query! client {:text (str "INSERT INTO " table " (id, note) VALUES (2, 'gone')")})
                    inside (pg/query! client {:text (str "SELECT count(*) AS n FROM " table)})
                    _ (pg/query! client {:text "ROLLBACK"})
                    after (pg/query! client {:text (str "SELECT count(*) AS n FROM " table)})]
              (expect "count inside / after / status" ["2" "1" :idle]
                      [(get (first (:rows inside)) "n") (get (first (:rows after)) "n")
                       (pg/transaction-status client)])))))

(defn- check-failed-block! [client]
  (check! "a rejected statement fails the block until ROLLBACK"
          (fn []
            (p/let [_ (pg/query! client {:text "BEGIN"})
                    d (failure-data (pg/query! client {:text (str "INSERT INTO " table " (id, note) VALUES (1, 'twice')")}))
                    s-failed (pg/transaction-status client)
                    d2 (failure-data (pg/query! client {:text "SELECT 1"}))
                    _ (pg/query! client {:text "ROLLBACK"})]
              (expect "type, code, constraint, status, next code"
                      [:pg-cljs/query-error "23505" (str table "_pkey") :in-failed-transaction "25P02" :idle]
                      [(:type d) (:code d) (:constraint d) s-failed (:code d2) (pg/transaction-status client)])))))

(defn- check-prepared! [client]
  (check! "a named prepared statement re-executes, by name alone the third time"
          (fn []
            (let [cmd {:name "pg-cljs-smoke-by-id"
                       :text (str "SELECT note FROM " table " WHERE id = $1")
                       :values [1]}]
              (p/let [a (pg/query! client cmd)
                      b (pg/query! client cmd)
                      c (pg/query! client (dissoc cmd :text))]
                (expect "notes" ["committed" "committed" "committed"]
                        (mapv #(get (first (:rows %)) "note") [a b c])))))))

(defn- check-pool! []
  (check! "pool: connect!, query!, with-client!, status, end!"
          (fn []
            (let [pool (pool/pool! (assoc config :max 2 :allow-exit-on-idle true))]
              (p/let [_ (pool/connect! pool)
                      q (pool/query! pool {:text "SELECT 1 AS n"})
                      w (pool/with-client! pool
                          (fn [c]
                            (p/let [r (pg/query! c {:text "SELECT current_setting('application_name') AS app"})]
                              (get (first (:rows r)) "app"))))
                      counts (select-keys (pool/status pool) [:total-count :idle-count :waiting-count])
                      ended (pool/end! pool)]
                (expect "query, app name, counts, ended"
                        [[{"n" 1}] "pg-cljs-smoke" {:total-count 1 :idle-count 1 :waiting-count 0} nil]
                        [(:rows q) w counts ended]))))))

(defn- check-listen! [client]
  (check! "LISTEN / NOTIFY from a second connection, with and without payload"
          (fn []
            (p/let [worker (pg/connect! config)
                    ch (listen/listen! client channel)
                    _ (pg/query! worker {:text "SELECT pg_notify($1, $2)" :values [channel "hello"]})
                    first-one (take-or-timeout ch 5000)
                    _ (pg/query! worker {:text (str "NOTIFY " channel)})
                    second-one (take-or-timeout ch 5000)
                    names (listen/listening client)
                    _ (listen/unlisten! client channel)
                    _ (pg/end! worker)]
              (expect "payloads, names after, open names"
                      [["hello" ""] #{channel} #{}]
                      [[(:payload first-one) (:payload second-one)] names (listen/listening client)])))))

(defn- check-notices! [client]
  (check! "notices! receives a RAISE NOTICE"
          (fn []
            (let [ch (listen/notices! client)]
              (p/let [_ (pg/query! client {:text "DO $$ BEGIN RAISE NOTICE 'pg-cljs smoke notice'; END $$"})
                      n (take-or-timeout ch 5000)]
                (async/close! ch)
                (expect "severity and message" ["NOTICE" "pg-cljs smoke notice"]
                        [(:severity n) (:message n)]))))))

(defn- check-error-path! [client]
  (check! "a missing relation is a query-error with SQLSTATE 42P01, connection still usable"
          (fn []
            (p/let [d (failure-data (pg/query! client {:text "SELECT * FROM pg_cljs_smoke_missing"}))
                    r (pg/query! client {:text "SELECT 1 AS n"})]
              (expect "type, code, severity, next rows, status"
                      [:pg-cljs/query-error "42P01" "ERROR" [{"n" 1}] :idle]
                      [(:type d) (:code d) (:severity d) (:rows r) (pg/transaction-status client)])))))

(defn- check-query-timeout! []
  (check! "query read timeout: the query rejects, the server keeps running it"
          (fn []
            (p/let [c (pg/connect! config)
                    d (failure-data (pg/query! c {:text "SELECT pg_sleep(1)" :query-timeout-ms 200}))
                    started (system-time)
                    _ (pg/query! c {:text "SELECT 1"})
                    waited (- (system-time) started)
                    _ (pg/end! c)]
              {:ok? (and (= :pg-cljs/query-error (:type d))
                         (= "Query read timeout" (:message d))
                         (> waited 500))
               :evidence (str (pr-str (:message d)) ", the next query waited "
                              (Math/round waited) " ms for pg_sleep to finish")}))))

(defn- cleanup! [client]
  (check! "cleanup: drop the temp table"
          (fn []
            (p/let [r (pg/query! client {:text (str "DROP TABLE IF EXISTS " table)})]
              (expect "command" "DROP" (:command r))))))

(defn- check-end! [client]
  (check! "end! resolves nil, is idempotent, and the handle refuses queries after"
          (fn []
            (p/let [a (pg/end! client)
                    b (pg/end! client)
                    d (failure-data (pg/query! client {:text "SELECT 1"}))]
              (expect "ends and the error after" [nil nil :pg-cljs/connect-error "Client was closed and is not queryable"]
                      [a b (:type d) (:message d)])))))

;; ---------------------------------------------------------------------------
;; Entry point
;; ---------------------------------------------------------------------------

(defn- summary! []
  (let [ok (count (filter :ok? @results))
        failed (- (count @results) ok)]
    (say (str ok " ok, " failed " failed"))
    (set! (.-exitCode js/process) (if (zero? failed) 0 1))))

(defn main [& _]
  (when-let [path (env secrets-file-variable)]
    (load-secrets-file! path))
  (say "pg-cljs smoke against" (pr-str (dissoc config :password)))
  (-> (p/let [client (-> (pg/connect! config)
                         (p/catch (fn [e]
                                    (record! "connect" false
                                             (str (ex-message e) " " (pr-str (dissoc (ex-data e) :cause))))
                                    nil)))]
        (if (nil? client)
          (say "no connection: the remaining checks are skipped")
          (p/let [_ (check-connect! client)
                  _ (check-temp-table! client)
                  _ (check-roundtrip! client)
                  _ (check-parameterized! client)
                  _ (check-transaction! client)
                  _ (check-rollback! client)
                  _ (check-failed-block! client)
                  _ (check-prepared! client)
                  _ (check-pool!)
                  _ (check-listen! client)
                  _ (check-notices! client)
                  _ (check-error-path! client)
                  _ (check-query-timeout!)
                  _ (cleanup! client)
                  _ (check-end! client)]
            nil)))
      (p/catch (fn [e]
                 (record! "smoke" false (str "unexpected: " (ex-message e) " " (pr-str (dissoc (ex-data e) :cause))))))
      (p/finally (fn [_ _] (summary!)))))
