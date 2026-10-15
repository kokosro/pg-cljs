(ns pg-cljs.core-test
  "The client surface against the wire-protocol fake server
   (pg-cljs.fake-server): the REAL pg Client speaks to a scripted socket,
   so connect!, query! and end! run end to end with no PostgreSQL server
   and no network. The only socket ever opened goes to 127.0.0.1:9, which
   refuses."
  (:require [cljs.test :refer [deftest is testing async]]
            [clojure.string :as str]
            [promesa.core :as p]
            [pg-cljs.binding :as b]
            [pg-cljs.core :as core]
            [pg-cljs.fake-server :as fake]
            [pg-cljs.util :as util]))

(deftest api-version-is-exact
  (is (= "0.1.0" core/api-version)))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- run
  "Runs the promise of (f) as an async test body; an unexpected rejection
   is a failed assertion."
  [done f]
  (-> (f)
      (p/catch (fn [e] (is false (str "unexpected rejection: " (ex-message e) " " (pr-str (ex-data e))))))
      (p/finally (fn [_ _] (done)))))

(defn- failure
  "Promise of the ex-data of the rejection of `pr` (nil when it resolved)."
  [pr]
  (-> pr
      (p/then (fn [v] (is false (str "expected a rejection, got " (pr-str v))) nil))
      (p/catch (fn [e] (ex-data e)))))

(defn- config
  "A connect! config pointing at the fake server."
  [server & [more]]
  (merge {:host "fake-host" :port 5432 :user "tester" :database "testdb"
          :stream (fake/stream-option server)}
         more))

(def ^:private people-fields
  [{:name "id" :data-type-id 23 :table-id 16384 :column-id 1 :data-type-size 4}
   {:name "name"}
   {:name "price_cents" :data-type-id 20}
   {:name "my col" :data-type-id 16}])

(def ^:private people-script
  {"SELECT id, name, price_cents, \"my col\" FROM people"
   {:fields people-fields
    :rows [["1" "ann" "12500" "t"]
           ["2" nil "9007199254740993" "f"]]
    :command "SELECT 2"}})

;; ---------------------------------------------------------------------------
;; Happy path
;; ---------------------------------------------------------------------------

(deftest connect-query-end-against-the-fake-server
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script people-script})
              text "SELECT id, name, price_cents, \"my col\" FROM people"]
          (p/let [client (core/connect! (config server))
                  r (core/query! client {:text text})
                  ended (core/end! client)]
            (testing "the handle"
              (is (core/client? client))
              (is (not (core/client? {:js nil})))
              (is (= :ended (:status @(:state client))))
              (is (= "#pg-cljs/client{:host \"fake-host\", :port 5432, :database \"testdb\", :user \"tester\", :status :ended}"
                     (pr-str client))))
            (testing "the startup the real Client sent"
              (is (= [{"user" "tester" "database" "testdb" "client_encoding" "UTF8"}]
                     (fake/startups server)))
              (is (= [{:port 5432 :host "fake-host"}] (fake/connects server))))
            (testing "the simple-protocol query, recorded verbatim"
              (is (= [{:text text :values nil :name nil :protocol :simple}]
                     (fake/queries server))))
            (testing "rows are maps keyed by the exact column-name strings; values as pg yields them"
              (is (= [{"id" 1 "name" "ann" "price_cents" "12500" "my col" true}
                      {"id" 2 "name" nil "price_cents" "9007199254740993" "my col" false}]
                     (:rows r))))
            (testing "fields keywordized, format as pg spells it"
              (is (= [{:name "id" :table-id 16384 :column-id 1 :data-type-id 23 :data-type-size 4
                       :data-type-modifier -1 :format "text"}
                      {:name "name" :table-id 0 :column-id 0 :data-type-id 25 :data-type-size -1
                       :data-type-modifier -1 :format "text"}
                      {:name "price_cents" :table-id 0 :column-id 0 :data-type-id 20 :data-type-size -1
                       :data-type-modifier -1 :format "text"}
                      {:name "my col" :table-id 0 :column-id 0 :data-type-id 16 :data-type-size -1
                       :data-type-modifier -1 :format "text"}]
                     (:fields r))))
            (testing "the envelope"
              (is (= #{:rows :fields :command :row-count :oid} (set (keys r))))
              (is (= "SELECT" (:command r)))
              (is (= 2 (:row-count r)))
              (is (nil? (:oid r))))
            (testing "end! resolves nil after Terminate"
              (is (nil? ended))
              (is (= 1 (fake/terminated-count server))))))))))

(deftest parameterized-values-travel-verbatim
  ;; Buffers would arrive as binary parameters (format 1) and Dates as
  ;; local-time ISO strings with offset (upstream prepareValue); neither
  ;; needs a JS literal here, strings / numbers / nil / booleans / vectors
  ;; show the extended protocol path.
  (async done
    (run done
      (fn []
        (let [text "INSERT INTO t(a, b, c, d, e) VALUES ($1, $2, $3, $4, $5)"
              server (fake/fake-server {:script {text {:command "INSERT 0 1"}}})]
          (p/let [client (core/connect! (config server))
                  r (core/query! client {:text text :values ["x y" 42 nil true [1 "a b" nil]]})
                  _ (core/end! client)]
            (is (= [{:text text
                     :values ["x y" "42" nil "true" "{\"1\",\"a b\",NULL}"]
                     :name nil
                     :protocol :extended}]
                   (fake/queries server)))
            (is (= {:rows [] :fields [] :command "INSERT" :row-count 1 :oid 0} r))))))))

(deftest row-mode-array-yields-vectors
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script people-script})
              text "SELECT id, name, price_cents, \"my col\" FROM people"]
          (p/let [client (core/connect! (config server))
                  r (core/query! client {:text text :row-mode :array})
                  _ (core/end! client)]
            (is (= [[1 "ann" "12500" true] [2 nil "9007199254740993" false]] (:rows r)))
            (is (= "id" (:name (first (:fields r)))))))))))

(deftest named-statements-are-parsed-once
  (async done
    (run done
      (fn []
        (let [text "SELECT $1::int AS n"
              server (fake/fake-server {:script {text {:fields [{:name "n" :data-type-id 23}]
                                                        :rows [["7"]]}}})]
          (p/let [client (core/connect! (config server))
                  r1 (core/query! client {:name "by-n" :text text :values [7]})
                  r2 (core/query! client {:name "by-n" :values [8]})
                  _ (core/end! client)]
            (is (= [{"n" 7}] (:rows r1)))
            (is (= [{"n" 7}] (:rows r2)))
            (testing "the second execution carried no Parse; the fake resolved the text by name"
              (is (= [{:text text :values ["7"] :name "by-n" :protocol :extended}
                      {:text text :values ["8"] :name "by-n" :protocol :extended}]
                     (fake/queries server))))))))))

(deftest a-multi-statement-text-resolves-a-vector-of-results
  (async done
    (run done
      (fn []
        (let [text "SELECT 1 AS a; INSERT INTO t VALUES (1); SELECT 'x' AS b"
              server (fake/fake-server
                      {:script {text [{:fields [{:name "a" :data-type-id 23}] :rows [["1"]] :command "SELECT 1"}
                                      {:command "INSERT 0 1"}
                                      {:fields ["b"] :rows [["x"]] :command "SELECT 1"}]}})]
          (p/let [client (core/connect! (config server))
                  r (core/query! client {:text text})
                  _ (core/end! client)]
            (is (vector? r))
            (is (= [{:rows [{"a" 1}] :fields [{:name "a" :table-id 0 :column-id 0 :data-type-id 23
                                               :data-type-size -1 :data-type-modifier -1 :format "text"}]
                     :command "SELECT" :row-count 1 :oid nil}
                    {:rows [] :fields [] :command "INSERT" :row-count 1 :oid 0}
                    {:rows [{"b" "x"}] :fields [{:name "b" :table-id 0 :column-id 0 :data-type-id 25
                                                 :data-type-size -1 :data-type-modifier -1 :format "text"}]
                     :command "SELECT" :row-count 1 :oid nil}]
                   r))))))))

(deftest a-connection-string-is-parsed-by-pg
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script {"SELECT 1" {:fields ["?column?"] :rows [["1"]]}}})]
          (p/let [client (core/connect! {:connection-string "postgres://alice:s3cret@db.internal:5433/billing?application_name=inv"
                                         :stream (fake/stream-option server)})
                  r (core/query! client {:text "SELECT 1"})
                  _ (core/end! client)]
            (is (= [{:port 5433 :host "db.internal"}] (fake/connects server)))
            (is (= [{"user" "alice" "database" "billing" "application_name" "inv" "client_encoding" "UTF8"}]
                   (fake/startups server)))
            (is (= [{"?column?" "1"}] (:rows r)))
            (testing "printing the handle never shows the password"
              (is (not (str/includes? (pr-str client) "s3cret")))
              (is (str/includes? (pr-str client) ":host \"db.internal\"")))))))))

(deftest nil-config-is-the-from-env-contract
  (is (= {} (core/from-env)))
  (is (= {} (util/connect-config nil)))
  (is (= {:application-name "x"} (merge (core/from-env) {:application-name "x"}))))

;; ---------------------------------------------------------------------------
;; Server errors
;; ---------------------------------------------------------------------------

(deftest an-error-response-is-a-query-error-with-the-sqlstate-fields
  (async done
    (run done
      (fn []
        (let [text "SELECT * FROM missing"
              server (fake/fake-server
                      {:script {text {:error {:severity "ERROR" :code "42P01"
                                              :message "relation \"missing\" does not exist"
                                              :position "15" :file "parse_relation.c" :line "1392"
                                              :routine "parserOpenTable"}}
                                "SELECT 2" {:fields ["n"] :rows [["2"]]}}})]
          (p/let [client (core/connect! (config server))
                  d (failure (core/query! client {:text text}))
                  again (core/query! client {:text "SELECT 2"})
                  _ (core/end! client)]
            (is (= :pg-cljs/query-error (:type d)))
            (is (= {:message "relation \"missing\" does not exist"
                    :severity "ERROR" :code "42P01" :position "15"
                    :file "parse_relation.c" :line "1392" :routine "parserOpenTable"
                    :op :query}
                   (dissoc d :type :cause :name)))
            (is (b/database-error? (:cause d)))
            (testing "the connection stays usable"
              (is (= [{"n" "2"}] (:rows again))))))))))

(deftest a-unique-violation-carries-schema-table-constraint
  (async done
    (run done
      (fn []
        (let [text "INSERT INTO users(id) VALUES ($1)"
              server (fake/fake-server
                      {:script {text {:error {:severity "ERROR" :code "23505"
                                              :message "duplicate key value violates unique constraint \"users_pkey\""
                                              :detail "Key (id)=(1) already exists."
                                              :schema "public" :table "users" :constraint "users_pkey"}}}})]
          (p/let [client (core/connect! (config server))
                  d (failure (core/query! client {:text text :values [1]}))
                  _ (core/end! client)]
            (is (= :pg-cljs/query-error (:type d)))
            (is (= {:code "23505" :detail "Key (id)=(1) already exists."
                    :schema "public" :table "users" :constraint "users_pkey"}
                   (select-keys d [:code :detail :schema :table :constraint])))))))))

(deftest the-query-read-timeout-is-a-query-error
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script {"SELECT pg_sleep(10)" {:silent? true}}})]
          (p/let [client (core/connect! (config server))
                  d (failure (core/query! client {:text "SELECT pg_sleep(10)" :query-timeout-ms 20}))
                  ;; the statement is still "running" server-side: end! destroys the socket
                  ended (core/end! client)]
            (is (= :pg-cljs/query-error (:type d)))
            (is (= "Query read timeout" (:message d)))
            (is (= :query (:op d)))
            (is (nil? ended))))))))

;; ---------------------------------------------------------------------------
;; Argument and config errors: nothing is sent
;; ---------------------------------------------------------------------------

(deftest unknown-config-keys-reject-before-any-connection
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {})]
          (p/let [d1 (failure (core/connect! (config server {:connectionTimeoutMillis 5})))
                  d2 (failure (core/connect! 42))
                  d3 (failure (core/connect! (config server {:ssl {:reject-unauthorized false :ca-file "x"}})))]
            (is (= :pg-cljs/config-error (:type d1)))
            (is (= #{:connectionTimeoutMillis} (:unknown d1)))
            (is (= :pg-cljs/config-error (:type d2)))
            (is (= 42 (:value d2)))
            (is (= :pg-cljs/config-error (:type d3)))
            (is (= #{:ca-file} (:unknown d3)))
            (testing "the stream factory was never called"
              (is (= [] (fake/connects server)))
              (is (= [] (fake/startups server))))))))))

(deftest bad-command-shapes-reject-before-anything-is-sent
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {})]
          (p/let [client (core/connect! (config server))
                  d1 (failure (core/query! client "SELECT 1"))
                  d2 (failure (core/query! client {:values [1]}))
                  d3 (failure (core/query! client {:text "x" :values "nope"}))
                  d4 (failure (core/query! client {:text "x" :row-mode :object}))
                  d5 (failure (core/query! client {:text "x" :values [{:a 1}]}))
                  d6 (failure (core/query! client nil))
                  d7 (failure (core/query! "not a client" {:text "x"}))
                  d8 (failure (core/end! :nope))
                  _ (core/end! client)]
            (is (= [:pg-cljs/config-error :cmd] [(:type d1) (:arg d1)]))
            (is (= [:pg-cljs/config-error :cmd] [(:type d2) (:arg d2)]))
            (is (= [:pg-cljs/config-error :values] [(:type d3) (:arg d3)]))
            (is (= [:pg-cljs/config-error :row-mode] [(:type d4) (:arg d4)]))
            (is (= [:pg-cljs/config-error :values 0] [(:type d5) (:arg d5) (:index d5)]))
            (is (= [:pg-cljs/config-error :cmd] [(:type d6) (:arg d6)]))
            (is (= [:pg-cljs/config-error :client :query] [(:type d7) (:arg d7) (:op d7)]))
            (is (= [:pg-cljs/config-error :client :end] [(:type d8) (:arg d8) (:op d8)]))
            (is (= [] (fake/queries server)))))))))

;; ---------------------------------------------------------------------------
;; Connection failures
;; ---------------------------------------------------------------------------

(deftest a-refused-connection-is-a-connect-error
  (async done
    (run done
      (fn []
        (p/let [d (failure (core/connect! {:host "127.0.0.1" :port 9 :user "u" :database "d"}))]
          (is (= :pg-cljs/connect-error (:type d)))
          (is (= "ECONNREFUSED" (:code d)))
          (is (= "connect" (:syscall d)))
          (is (= "127.0.0.1" (:address d)))
          (is (= 9 (:port d)))
          (is (= :connect (:op d)))
          (is (str/includes? (:message d) "ECONNREFUSED"))
          (is (some? (:cause d))))))))

(deftest the-connection-timeout-is-a-connect-error
  ;; The Client's connection timer is unref'd, so the fake holds the event
  ;; loop open (:hold-ms) long enough for it to fire.
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:startup {:silent? true} :hold-ms 300})]
          (p/let [d (failure (core/connect! (config server {:connection-timeout-ms 30})))]
            (is (= :pg-cljs/connect-error (:type d)))
            (is (= "timeout expired" (:message d)))
            (is (= :connect (:op d)))
            (is (= 1 (count (fake/startups server))))))))))

(deftest a-startup-error-response-is-a-connect-error-with-the-sqlstate
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:startup {:silent? true} :hold-ms 50})
              ;; the fake answers startup itself only with the happy path; push the
              ;; server's refusal by hand once the startup arrived
              pr (core/connect! (config server))]
          (js/setTimeout
           (fn []
             (doseq [s @(:streams server)]
               (.push s (js/Buffer.concat
                         #js [(fake/error-response {:severity "FATAL" :code "28P01"
                                                    :message "password authentication failed for user \"tester\""})]))))
           10)
          (p/let [d (failure pr)]
            (is (= :pg-cljs/connect-error (:type d)))
            (is (= {:severity "FATAL" :code "28P01"
                    :message "password authentication failed for user \"tester\"" :op :connect}
                   (dissoc d :type :cause :name)))))))))

(deftest a-query-after-end-is-a-connect-error
  ;; Upstream (lib/client.js, query): after end() the client has _ending
  ;; set and rejects every new query on the next tick with the plain Error
  ;; "Client was closed and is not queryable"; the connection is gone, so
  ;; pg-cljs reports it as a connect-error with :op :query.
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script {"SELECT 1" {:fields ["n"] :rows [["1"]]}}})]
          (p/let [client (core/connect! (config server))
                  _ (core/end! client)
                  d (failure (core/query! client {:text "SELECT 1"}))
                  again (core/end! client)]
            (is (= :pg-cljs/connect-error (:type d)))
            (is (= "Client was closed and is not queryable" (:message d)))
            (is (= :query (:op d)))
            (testing "end! is idempotent: upstream resolves at once when already ended"
              (is (nil? again))
              (is (= 1 (fake/terminated-count server))))
            (is (= [] (fake/queries server)))))))))

(deftest end-during-an-active-query-terminates-it
  ;; Upstream end() with an active (non-pipeline) query destroys the socket
  ;; instead of draining; the query rejects "Connection terminated" and no
  ;; `error` event is emitted (the client is ending on purpose).
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script {"SELECT slow()" {:fields ["n"] :rows [["1"]] :delay-ms 200}}})
              started (atom nil)]
          (p/let [client (core/connect! (config server))
                  ;; start the query without awaiting it (p/let awaits every binding)
                  pending (do (reset! started (failure (core/query! client {:text "SELECT slow()"}))) nil)
                  ended (core/end! client)
                  d @started]
            (is (nil? pending))
            (is (nil? ended))
            (is (= :pg-cljs/connect-error (:type d)))
            (is (= "Connection terminated" (:message d)))
            (is (= :query (:op d)))
            (is (= 0 (fake/terminated-count server)) "the socket was destroyed, no Terminate was sent")))))))

(deftest a-dropped-connection-rejects-the-active-query
  ;; Upstream emits `error` on the client when the socket closes without
  ;; end(); pg-cljs attaches no listener of its own (a Node EventEmitter
  ;; with none would throw), so the test subscribes through the binding.
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script {"SELECT slow()" {:fields ["n"] :rows [["1"]] :delay-ms 200}}})
              errors (atom [])]
          (p/let [client (core/connect! (config server))
                  _ (b/on (:js client) "error" (fn [e] (swap! errors conj (:message (b/error-info e)))))
                  d (do (js/setTimeout #(fake/drop-connections! server) 10)
                        (failure (core/query! client {:text "SELECT slow()"})))
                  ended (core/end! client)]
            (is (= :pg-cljs/connect-error (:type d)))
            (is (= "Connection terminated unexpectedly" (:message d)))
            (is (= ["Connection terminated unexpectedly"] @errors))
            (is (nil? ended))
            (is (= :ended (:status @(:state client))))))))))

;; ---------------------------------------------------------------------------
;; Events the binding will feed the Phase 2 channels
;; ---------------------------------------------------------------------------

(deftest notifications-and-notices-arrive-as-data
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server
                      {:process-id 4242
                       :script {"LISTEN jobs" {:command "LISTEN"}
                                "NOTIFY jobs, 'hello'" {:command "NOTIFY"
                                                        :notifications [{:process-id 4242 :channel "jobs" :payload "hello"}]
                                                        :notices [{:severity "NOTICE" :code "00000" :message "fyi"}]}}})
              heard (atom [])
              noticed (atom [])]
          (p/let [client (core/connect! (config server))
                  _ (b/on (:js client) "notification" (fn [msg] (swap! heard conj (b/notification->data msg))))
                  _ (b/on (:js client) "notice" (fn [msg] (swap! noticed conj (b/notice->data msg))))
                  r1 (core/query! client {:text "LISTEN jobs"})
                  r2 (core/query! client {:text "NOTIFY jobs, 'hello'"})
                  _ (do (fake/notify! server {:process-id 99 :channel "jobs" :payload ""})
                        (p/delay 20))
                  _ (core/end! client)]
            (is (= {:rows [] :fields [] :command "LISTEN" :row-count nil :oid nil} r1))
            (is (= "NOTIFY" (:command r2)))
            (is (= [{:process-id 4242 :channel "jobs" :payload "hello"}
                    {:process-id 99 :channel "jobs" :payload ""}]
                   @heard))
            (is (= [{:message "fyi" :severity "NOTICE" :code "00000"}] @noticed))
            (is (= "I" (b/transaction-status (:js client))))
            (is (= :idle (core/transaction-status client)))))))))
