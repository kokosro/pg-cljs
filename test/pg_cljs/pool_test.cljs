(ns pg-cljs.pool-test
  "The pool surface against the wire-protocol fake server: the REAL
   pg.Pool creates real pg Clients over scripted fake sockets (the
   :stream factory is forwarded to every client), so checkout, release,
   one-off queries, counters and end run end to end with no PostgreSQL
   server and no network."
  (:require [cljs.test :refer [deftest is testing async]]
            [clojure.string :as str]
            [promesa.core :as p]
            [pg-cljs.binding :as b]
            [pg-cljs.core :as core]
            [pg-cljs.fake-server :as fake]
            [pg-cljs.pool :as pool]
            [pg-cljs.util :as util]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- run
  [done f]
  (-> (f)
      (p/catch (fn [e] (is false (str "unexpected rejection: " (ex-message e) " " (pr-str (ex-data e))))))
      (p/finally (fn [_ _] (done)))))

(defn- failure
  [pr]
  (-> pr
      (p/then (fn [v] (is false (str "expected a rejection, got " (pr-str v))) nil))
      (p/catch (fn [e] (ex-data e)))))

(defn- thrown-data [f]
  (try (f) nil (catch :default e (ex-data e))))

(defn- config
  "A pool! config pointing at the fake server. :allow-exit-on-idle so an
   idle client of a failed test cannot hold the process open."
  [server & [more]]
  (merge {:host "fake-host" :port 5432 :user "tester" :database "testdb"
          :allow-exit-on-idle true
          :stream (fake/stream-option server)}
         more))

(def ^:private one-script
  {"SELECT 1 AS n" {:fields [{:name "n" :data-type-id 23}] :rows [["1"]]}
   "SELECT 2 AS n" {:fields [{:name "n" :data-type-id 23}] :rows [["2"]]}})

(defn- counts [pool]
  (select-keys (pool/status pool) [:total-count :idle-count :waiting-count]))

;; ---------------------------------------------------------------------------
;; Construction and validation: nothing is sent
;; ---------------------------------------------------------------------------

(deftest pool-is-created-without-connecting
  (let [server (fake/fake-server {})
        pool (pool/pool! (config server {:max 3 :min 1 :idle-timeout-ms 0}))]
    (is (pool/pool? pool))
    (is (not (pool/pool? {:js nil})))
    (is (= {:total-count 0 :idle-count 0 :waiting-count 0 :expired-count 0
            :ending? false :ended? false :max 3 :min 1}
           (pool/status pool)))
    (is (= "#pg-cljs/pool{:host \"fake-host\", :port 5432, :database \"testdb\", :user \"tester\", :max 3, :total-count 0, :idle-count 0, :status :open}"
           (pr-str pool)))
    (is (= [] (fake/connects server)))
    (testing "upstream defaults are what pg-pool resolves, not pg.defaults"
      (is (= {:max 10 :min 0} (select-keys (pool/status (pool/pool! (config server))) [:max :min]))))))

(deftest pool-config-is-validated-before-anything-is-sent
  (let [server (fake/fake-server {})
        d1 (thrown-data #(pool/pool! (config server {:max 2 :idleTimeoutMillis 5 :bogus 1})))
        d2 (thrown-data #(pool/pool! 42))
        d3 (thrown-data #(pool/pool! (config server {:ssl {:ca "x" :ca-file "y"}})))
        d4 (thrown-data #(pool/status "nope"))]
    (is (= :pg-cljs/config-error (:type d1)))
    (is (= #{:idleTimeoutMillis :bogus} (:unknown d1)))
    (is (= (set (keys util/pool-config-keys)) (:allowed d1)))
    (is (= :pool (:op d1)))
    (is (= [:pg-cljs/config-error :config :pool 42] [(:type d2) (:arg d2) (:op d2) (:value d2)]))
    (is (= [:pg-cljs/config-error :ssl #{:ca-file}] [(:type d3) (:arg d3) (:unknown d3)]))
    (is (= [:pg-cljs/config-error :pool] [(:type d4) (:arg d4)]))
    (is (= [] (fake/connects server)))
    (testing "a connection string and nil are accepted as for connect!"
      (is (pool/pool? (pool/pool! "postgres://u:p@h:5433/d")))
      (is (pool/pool? (pool/pool! nil))))
    (testing "printing a pool never shows the password"
      (let [printed (pr-str (pool/pool! {:host "h" :password "s3cret" :max 1}))]
        (is (not (str/includes? printed "s3cret")))
        (is (str/includes? printed ":host \"h\""))))))

;; ---------------------------------------------------------------------------
;; Eager connect, one-off queries, end
;; ---------------------------------------------------------------------------

(deftest connect-query-end
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script one-script})
              pool (pool/pool! (config server {:max 2}))]
          (p/let [p1 (pool/connect! pool)
                  after-connect (counts pool)
                  r1 (pool/query! pool {:text "SELECT 1 AS n"})
                  r2 (pool/pool-query! pool {:text "SELECT 2 AS n"})
                  after-queries (counts pool)
                  ended (pool/end! pool)
                  d-query (failure (pool/query! pool {:text "SELECT 1 AS n"}))
                  d-acquire (failure (pool/acquire! pool))
                  d-end (failure (pool/end! pool))]
            (testing "connect! resolves with the pool once a first client sits idle"
              (is (identical? pool p1))
              (is (= {:total-count 1 :idle-count 1 :waiting-count 0} after-connect))
              (is (= 1 (count (fake/startups server)))))
            (testing "query! reuses the idle client and resolves the client result shape"
              (is (= [{"n" 1}] (:rows r1)))
              (is (= {:rows [{"n" 2}] :command "SELECT" :row-count 1 :oid nil}
                     (dissoc r2 :fields)))
              (is (= {:total-count 1 :idle-count 1 :waiting-count 0} after-queries))
              (is (= 1 (count (fake/startups server))))
              (is (= ["SELECT 1 AS n" "SELECT 2 AS n"] (map :text (fake/queries server)))))
            (testing "end! ends the idle client (Terminate sent) and resolves nil"
              (is (nil? ended))
              (is (= 1 (fake/terminated-count server)))
              (is (= {:total-count 0 :idle-count 0 :waiting-count 0 :expired-count 0
                      :ending? true :ended? true :max 2 :min 0}
                     (pool/status pool)))
              (is (str/ends-with? (pr-str pool) ":status :ended}")))
            (testing "after end!: upstream's wording, as connect-errors"
              (is (= [:pg-cljs/connect-error "Cannot use a pool after calling end on the pool" :query]
                     [(:type d-query) (:message d-query) (:op d-query)]))
              (is (= [:pg-cljs/connect-error "Cannot use a pool after calling end on the pool" :acquire]
                     [(:type d-acquire) (:message d-acquire) (:op d-acquire)]))
              (is (= [:pg-cljs/connect-error "Called end on pool more than once" :end]
                     [(:type d-end) (:message d-end) (:op d-end)])))))))))

(deftest a-failed-pool-query-costs-a-reconnect
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server
                      {:script (assoc one-script
                                      "SELECT * FROM missing"
                                      {:error {:severity "ERROR" :code "42P01"
                                               :message "relation \"missing\" does not exist"}})})
              pool (pool/pool! (config server {:max 2}))
              events (atom [])]
          (b/on (:js pool) "release" (fn [err _] (swap! events conj [:release (when err (:code (b/error-info err)))])))
          (b/on (:js pool) "remove" (fn [_] (swap! events conj [:remove])))
          (p/let [r1 (pool/query! pool {:text "SELECT 1 AS n"})
                  d (failure (pool/query! pool {:text "SELECT * FROM missing"}))
                  _ (p/delay 10)
                  after-failure (counts pool)
                  events-after-failure @events
                  r2 (pool/query! pool {:text "SELECT 2 AS n"})
                  _ (pool/end! pool)]
            (is (= [{"n" 1}] (:rows r1)))
            (is (= [:pg-cljs/query-error "42P01" :query] [(:type d) (:code d) (:op d)]))
            (testing "the client was released with the error and removed"
              (is (= [[:release nil] [:release "42P01"] [:remove]] events-after-failure))
              (is (= [:release nil] (last @events)) "the reconnecting query released its client normally")
              (is (= {:total-count 0 :idle-count 0 :waiting-count 0} after-failure)))
            (testing "the next query connected a new client"
              (is (= [{"n" 2}] (:rows r2)))
              (is (= 2 (count (fake/startups server)))))))))))

(deftest a-refused-pool-connection-is-a-connect-error
  (async done
    (run done
      (fn []
        (let [pool (pool/pool! {:host "127.0.0.1" :port 9 :user "u" :database "d" :allow-exit-on-idle true})]
          (p/let [d1 (failure (pool/connect! pool))
                  d2 (failure (pool/query! pool {:text "SELECT 1"}))
                  _ (pool/end! pool)]
            (is (= [:pg-cljs/connect-error "ECONNREFUSED" :acquire] [(:type d1) (:code d1) (:op d1)]))
            (is (= [:pg-cljs/connect-error "ECONNREFUSED" :query] [(:type d2) (:code d2) (:op d2)]))
            (is (= {:total-count 0 :idle-count 0 :waiting-count 0} (counts pool)))))))))

;; ---------------------------------------------------------------------------
;; Checkout and release
;; ---------------------------------------------------------------------------

(deftest acquire-release-round-trip
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script (assoc one-script
                                                       "BEGIN" {:command "BEGIN" :status "T"}
                                                       "COMMIT" {:command "COMMIT" :status "I"})})
              pool (pool/pool! (config server {:max 2}))]
          (p/let [client (pool/acquire! pool)
                  held (counts pool)
                  _ (core/query! client {:text "BEGIN"})
                  tx (core/transaction-status client)
                  r (core/query! client {:text "SELECT 1 AS n"})
                  _ (core/query! client {:text "COMMIT"})
                  released (pool/release! client)
                  idle (counts pool)
                  d-twice (failure (pool/release! client))
                  d-foreign (failure (pool/release! "nope"))
                  again (pool/acquire! pool)
                  _ (pool/release! again)
                  _ (pool/end! pool)]
            (testing "the checked-out client is a core client handle"
              (is (core/client? client))
              (is (= "#pg-cljs/client{:host \"fake-host\", :port 5432, :database \"testdb\", :user \"tester\", :status :released}"
                     (pr-str client)))
              (is (= {:total-count 1 :idle-count 0 :waiting-count 0} held))
              (is (= :in-transaction tx))
              (is (= [{"n" 1}] (:rows r)))
              (is (= ["BEGIN" "SELECT 1 AS n" "COMMIT"] (map :text (fake/queries server)))))
            (testing "release! returns it to the idle set"
              (is (nil? released))
              (is (= {:total-count 1 :idle-count 1 :waiting-count 0} idle)))
            (testing "a second release! and a non-pooled client are config-errors"
              (is (= [:pg-cljs/config-error :client :release] [(:type d-twice) (:arg d-twice) (:op d-twice)]))
              (is (= [:pg-cljs/config-error :client :release] [(:type d-foreign) (:arg d-foreign) (:op d-foreign)])))
            (testing "the same connection is handed out again"
              (is (= 1 (count (fake/startups server))))
              (is (identical? (:js client) (:js again))))))))))

(deftest release-with-destroy-ends-the-connection
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script one-script})
              pool (pool/pool! (config server {:max 2}))
              releases (atom [])]
          (b/on (:js pool) "release" (fn [err _] (swap! releases conj err)))
          (p/let [c1 (pool/acquire! pool)
                  _ (pool/release! c1 true)
                  _ (p/delay 10)
                  after-destroy (counts pool)
                  terminated-after-destroy (fake/terminated-count server)
                  c2 (pool/acquire! pool)
                  _ (pool/release! c2 (ex-info "done with it" {:why :test}))
                  _ (p/delay 10)
                  after-error (counts pool)
                  _ (pool/end! pool)]
            (is (= {:total-count 0 :idle-count 0 :waiting-count 0} after-destroy))
            (is (= 1 terminated-after-destroy))
            (is (= :released (:status @(:state c1))) "the end listener was detached at release: the handle stays :released")
            (is (= {:total-count 0 :idle-count 0 :waiting-count 0} after-error))
            (is (= 2 (fake/terminated-count server)))
            (testing "the release value reaches the pool's release event verbatim"
              (is (= true (first @releases)))
              (is (= "done with it" (ex-message (second @releases)))))))))))

(deftest a-non-pooled-client-cannot-be-released
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {})]
          (p/let [client (core/connect! {:host "fake-host" :port 5432 :user "u" :database "d"
                                         :stream (fake/stream-option server)})
                  d (failure (pool/release! client))
                  _ (core/end! client)]
            (is (= :pg-cljs/config-error (:type d)))
            (is (= "release!: the client was not checked out of a pool" (:message d)))))))))

(deftest with-client-releases-on-success-and-failure
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script one-script})
              pool (pool/pool! (config server {:max 2}))
              seen (atom nil)]
          (p/let [v (pool/with-client! pool (fn [c]
                                              (reset! seen c)
                                              (p/let [r (core/query! c {:text "SELECT 1 AS n"})]
                                                (:rows r))))
                  after-ok (counts pool)
                  d (failure (pool/with-client! pool (fn [_] (throw (ex-info "boom" {:k 1})))))
                  after-throw (counts pool)
                  d2 (failure (pool/with-client! pool (fn [c] (core/query! c {:text "SELECT nope"}))))
                  after-reject (counts pool)
                  v3 (pool/with-client! pool (fn [c] (pool/release! c true) :destroyed-inside))
                  _ (p/delay 10)
                  after-inside (counts pool)
                  _ (pool/end! pool)]
            (is (= [{"n" 1}] v))
            (is (= :released (:status @(:state @seen))))
            (is (= {:total-count 1 :idle-count 1 :waiting-count 0} after-ok))
            (testing "a synchronous throw of the body releases and re-throws"
              (is (= {:k 1} d))
              (is (= {:total-count 1 :idle-count 1 :waiting-count 0} after-throw)))
            (testing "a rejected body releases and re-throws"
              (is (= :pg-cljs/query-error (:type d2)))
              (is (= {:total-count 1 :idle-count 1 :waiting-count 0} after-reject)))
            (testing "a body that released the client itself is left alone"
              (is (= :destroyed-inside v3))
              (is (= {:total-count 0 :idle-count 0 :waiting-count 0} after-inside)))))))))

(deftest a-full-pool-queues-acquires
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script one-script})
              pool (pool/pool! (config server {:max 2}))
              third (atom nil)]
          (p/let [c1 (pool/acquire! pool)
                  c2 (pool/acquire! pool)
                  ;; start the third acquire without awaiting it (p/let awaits every binding)
                  _ (do (reset! third (pool/acquire! pool)) nil)
                  _ (p/delay 10)
                  waiting (counts pool)
                  _ (pool/release! c1)
                  c3 @third
                  served (counts pool)
                  _ (pool/release! c2)
                  _ (pool/release! c3)
                  _ (pool/end! pool)]
            (is (= {:total-count 2 :idle-count 0 :waiting-count 1} waiting))
            (is (identical? (:js c1) (:js c3)) "the released client served the waiter")
            (is (= {:total-count 2 :idle-count 0 :waiting-count 0} served))
            (is (= 2 (count (fake/startups server))))
            (is (= 2 (fake/terminated-count server)))))))))

(deftest a-queue-wait-timeout-is-a-connect-error
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script one-script :hold-ms 200})
              pool (pool/pool! (config server {:max 1 :connection-timeout-ms 30}))]
          (p/let [c1 (pool/acquire! pool)
                  d (failure (pool/acquire! pool))
                  _ (pool/release! c1)
                  _ (pool/end! pool)]
            (is (= [:pg-cljs/connect-error "timeout exceeded when trying to connect" :acquire]
                   [(:type d) (:message d) (:op d)]))))))))

(deftest end-waits-for-checked-out-clients
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script one-script})
              pool (pool/pool! (config server {:max 2}))
              ended (atom false)]
          (p/let [c1 (pool/acquire! pool)
                  ending (do (-> (pool/end! pool) (p/then (fn [_] (reset! ended true)))) nil)
                  _ (p/delay 20)
                  while-held [(pool/status pool) @ended]
                  _ (pool/release! c1)
                  _ (p/delay 20)
                  after-release [(select-keys (pool/status pool) [:total-count :ending? :ended?]) @ended]]
            (is (nil? ending))
            (is (= [{:total-count 1 :idle-count 0 :waiting-count 0 :expired-count 0
                     :ending? true :ended? false :max 2 :min 0}
                    false]
                   while-held))
            (is (= [{:total-count 0 :ending? true :ended? true} true] after-release))
            (is (= 1 (fake/terminated-count server)))))))))

;; ---------------------------------------------------------------------------
;; An idle connection breaking: the pool's `error` event
;; ---------------------------------------------------------------------------

(deftest an-idle-connection-breaking-is-a-pool-error-event
  ;; Upstream removes the broken idle client and emits `error` on the
  ;; pool; pg-cljs attaches no listener (a Node EventEmitter with none
  ;; would throw), so the test subscribes through the binding.
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script one-script})
              pool (pool/pool! (config server {:max 2}))
              errors (atom [])]
          (b/on (:js pool) "error" (fn [e c] (swap! errors conj [(:message (b/error-info e)) (some? c)])))
          (p/let [_ (pool/connect! pool)
                  _ (do (fake/drop-connections! server) (p/delay 20))
                  after-drop (counts pool)
                  r (pool/query! pool {:text "SELECT 1 AS n"})
                  _ (pool/end! pool)]
            (is (= [["Connection terminated unexpectedly" true]] @errors))
            (is (= {:total-count 0 :idle-count 0 :waiting-count 0} after-drop))
            (testing "the pool reconnects for the next query"
              (is (= [{"n" 1}] (:rows r)))
              (is (= 2 (count (fake/startups server)))))))))))

;; ---------------------------------------------------------------------------
;; Aliases
;; ---------------------------------------------------------------------------

(deftest the-pool-prefixed-aliases-are-the-same-functions
  (is (identical? pool/connect! pool/pool-connect!))
  (is (identical? pool/query! pool/pool-query!))
  (is (identical? pool/end! pool/pool-end!))
  (is (identical? pool/status pool/pool-status)))
