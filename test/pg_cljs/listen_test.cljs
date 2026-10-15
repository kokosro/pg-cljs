(ns pg-cljs.listen-test
  "LISTEN/NOTIFY channels, notices and the transaction status against the
   wire-protocol fake server: the fake answers LISTEN / UNLISTEN like a
   server and pushes NotificationResponse / NoticeResponse messages out of
   band on every live connection (notify! / notice!)."
  (:require [cljs.test :refer [deftest is testing async]]
            [cljs.core.async :as async]
            [cljs.core.async.impl.protocols :as impl]
            [promesa.core :as p]
            [pg-cljs.core :as core]
            [pg-cljs.fake-server :as fake]
            [pg-cljs.listen :as listen]
            [pg-cljs.pool :as pool]))

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
  [server & [more]]
  (merge {:host "fake-host" :port 5432 :user "tester" :database "testdb"
          :stream (fake/stream-option server)}
         more))

(defn- drain!
  "Everything currently buffered on `ch`, without waiting, as a vector."
  [ch]
  (loop [acc []]
    (if-some [v (async/poll! ch)]
      (recur (conj acc v))
      acc)))

(defn- closed? [ch] (impl/closed? ch))

(def ^:private listen-script
  {"LISTEN \"jobs\"" {:command "LISTEN"}
   "UNLISTEN \"jobs\"" {:command "UNLISTEN"}
   "LISTEN \"Other\"" {:command "LISTEN"}
   "NOTIFY \"jobs\", 'hello'" {:command "NOTIFY"
                               :notifications [{:process-id 4242 :channel "jobs" :payload "hello"}]}
   "BEGIN" {:command "BEGIN" :status "T"}
   "COMMIT" {:command "COMMIT" :status "I"}
   "ROLLBACK" {:command "ROLLBACK" :status "I"}
   "SELECT 1/0" {:error {:severity "ERROR" :code "22012" :message "division by zero"} :status "E"}})

;; ---------------------------------------------------------------------------
;; Notifications
;; ---------------------------------------------------------------------------

(deftest notifications-flow-while-subscribed
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:process-id 4242 :script listen-script})]
          (p/let [client (core/connect! (config server))
                  ch (listen/listen! client "jobs")
                  r (core/query! client {:text "NOTIFY \"jobs\", 'hello'"})
                  _ (do (fake/notify! server {:process-id 99 :channel "jobs" :payload ""})
                        (fake/notify! server {:process-id 99 :channel "other" :payload "not mine"})
                        (p/delay 20))
                  got (drain! ch)
                  subscribed (listen/listening client)
                  unlistened (listen/unlisten! client "jobs")
                  _ (do (fake/notify! server {:process-id 99 :channel "jobs" :payload "late"})
                        (p/delay 20))
                  after (drain! ch)
                  _ (core/end! client)]
            (testing "the LISTEN went out quoted and the NOTIFY result is a plain command"
              (is (= ["LISTEN \"jobs\"" "NOTIFY \"jobs\", 'hello'" "UNLISTEN \"jobs\""]
                     (map :text (fake/queries server))))
              (is (= {:rows [] :fields [] :command "NOTIFY" :row-count nil :oid nil} r)))
            (testing "a notification during the NOTIFY query and one out of band, the other channel's filtered out"
              (is (= [{:process-id 4242 :channel "jobs" :payload "hello"}
                      {:process-id 99 :channel "jobs" :payload ""}]
                     got))
              (is (= #{"jobs"} subscribed)))
            (testing "unlisten! closes the channel; nothing arrives afterwards"
              (is (nil? unlistened))
              (is (closed? ch))
              (is (= [] after))
              (is (= #{} (listen/listening client))))))))))

(deftest the-listen-command-echo-and-a-failed-listen
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server
                      {:script (assoc listen-script
                                      "LISTEN \"bad name\""
                                      {:error {:severity "ERROR" :code "42601" :message "syntax error"}})})]
          (p/let [client (core/connect! (config server))
                  echo (core/query! client {:text "LISTEN \"jobs\""})
                  d (failure (listen/listen! client "bad name"))
                  _ (core/end! client)]
            (testing "a LISTEN sent through query! resolves the command echo, no rows"
              (is (= {:rows [] :fields [] :command "LISTEN" :row-count nil :oid nil} echo)))
            (testing "a refused LISTEN rejects as a query-error and leaves no subscription"
              (is (= [:pg-cljs/query-error "42601" :query] [(:type d) (:code d) (:op d)]))
              (is (= #{} (listen/listening client))))))))))

(deftest a-small-buffer-drops-the-oldest
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script listen-script})]
          (p/let [client (core/connect! (config server))
                  ch (listen/listen! client "jobs" {:buffer-size 2})
                  _ (do (doseq [i (range 5)]
                          (fake/notify! server {:process-id 1 :channel "jobs" :payload (str i)}))
                        (p/delay 20))
                  got (drain! ch)
                  _ (core/end! client)]
            (is (= ["3" "4"] (map :payload got)))))))))

(deftest only-the-subscribed-connection-and-name-get-the-notification
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script listen-script})]
          (p/let [a (core/connect! (config server))
                  b (core/connect! (config server))
                  ch-a (listen/listen! a "jobs")
                  ch-b (listen/listen! b "Other")
                  ;; the fake pushes on EVERY live connection, as a server
                  ;; would for two connections both LISTENing: the filter
                  ;; by name and the absence of a listen! on b for "jobs"
                  ;; are what keep b's channel empty
                  _ (do (fake/notify! server {:process-id 1 :channel "jobs" :payload "x"})
                        (p/delay 20))
                  got-a (drain! ch-a)
                  got-b (drain! ch-b)
                  _ (core/end! a)
                  _ (core/end! b)]
            (is (= [{:process-id 1 :channel "jobs" :payload "x"}] got-a))
            (is (= [] got-b))
            (is (= ["LISTEN \"jobs\"" "LISTEN \"Other\""] (map :text (fake/queries server))))
            (testing "ending the connections closes the channels"
              (is (closed? ch-a))
              (is (closed? ch-b)))))))))

(deftest closing-the-channel-yourself-detaches-at-the-next-notification
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script listen-script})]
          (p/let [client (core/connect! (config server))
                  ch (listen/listen! client "jobs")
                  _ (async/close! ch)
                  before (listen/listening client)
                  _ (do (fake/notify! server {:process-id 1 :channel "jobs" :payload "x"})
                        (p/delay 20))
                  after (listen/listening client)
                  _ (core/end! client)]
            (is (= #{"jobs"} before))
            (is (= #{} after))
            (is (= ["LISTEN \"jobs\""] (map :text (fake/queries server))) "no UNLISTEN was sent")))))))

(deftest listen-on-a-pooled-client
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script listen-script})
              pool (pool/pool! (config server {:max 1 :allow-exit-on-idle true}))]
          (p/let [client (pool/acquire! pool)
                  ch (listen/listen! client "jobs")
                  _ (do (fake/notify! server {:process-id 1 :channel "jobs" :payload "x"})
                        (p/delay 20))
                  got (drain! ch)
                  _ (listen/unlisten! client "jobs")
                  _ (pool/release! client)
                  _ (pool/end! pool)]
            (is (= ["x"] (map :payload got)))
            (is (closed? ch))))))))

(deftest listen-arguments-are-validated
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script listen-script})]
          (p/let [client (core/connect! (config server))
                  d1 (failure (listen/listen! client ""))
                  d2 (failure (listen/listen! client "jobs" {:buffer 5}))
                  d3 (failure (listen/listen! client "jobs" {:buffer-size 0}))
                  d4 (failure (listen/listen! "nope" "jobs"))
                  d5 (failure (listen/unlisten! client nil))
                  d6 (thrown-data #(listen/notices! client {:buffer-size -1}))
                  d7 (thrown-data #(listen/listening 42))
                  _ (core/end! client)]
            (is (= [:pg-cljs/config-error :channel :listen] [(:type d1) (:arg d1) (:op d1)]))
            (is (= [:pg-cljs/config-error :opts #{:buffer}] [(:type d2) (:arg d2) (:unknown d2)]))
            (is (= [:pg-cljs/config-error :buffer-size 0] [(:type d3) (:arg d3) (:value d3)]))
            (is (= [:pg-cljs/config-error :client :listen] [(:type d4) (:arg d4) (:op d4)]))
            (is (= [:pg-cljs/config-error :channel :unlisten] [(:type d5) (:arg d5) (:op d5)]))
            (is (= [:pg-cljs/config-error :buffer-size :notices] [(:type d6) (:arg d6) (:op d6)]))
            (is (= [:pg-cljs/config-error :client] [(:type d7) (:arg d7)]))
            (is (= [] (fake/queries server)))))))))

;; ---------------------------------------------------------------------------
;; Notices
;; ---------------------------------------------------------------------------

(deftest notices-flow-on-their-own-channel
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server
                      {:script (assoc listen-script
                                      "SELECT warn()" {:fields ["n"] :rows [["1"]]
                                                       :notices [{:severity "WARNING" :code "01000"
                                                                  :message "careful" :hint "or not"}]})})]
          (p/let [client (core/connect! (config server))
                  ch (listen/notices! client {:buffer-size 1})
                  jobs (listen/listen! client "jobs")
                  r (core/query! client {:text "SELECT warn()"})
                  _ (do (fake/notice! server {:severity "NOTICE" :code "00000" :message "fyi"})
                        (fake/notify! server {:process-id 1 :channel "jobs" :payload "n"})
                        (p/delay 20))
                  got (drain! ch)
                  got-jobs (drain! jobs)
                  _ (core/end! client)]
            (is (= [{"n" "1"}] (:rows r)) "a notice never rejects the query")
            (testing "sliding buffer of 1: only the newest notice is left"
              (is (= [{:message "fyi" :severity "NOTICE" :code "00000"}] got)))
            (testing "notifications and notices do not mix"
              (is (= [{:process-id 1 :channel "jobs" :payload "n"}] got-jobs)))
            (is (closed? ch))))))))

;; ---------------------------------------------------------------------------
;; Transaction status
;; ---------------------------------------------------------------------------

(deftest transaction-status-follows-ready-for-query
  (async done
    (run done
      (fn []
        (let [server (fake/fake-server {:script listen-script})]
          (p/let [client (core/connect! (config server))
                  s0 (core/transaction-status client)
                  _ (core/query! client {:text "BEGIN"})
                  s1 (core/transaction-status client)
                  d (failure (core/query! client {:text "SELECT 1/0"}))
                  s2 (core/transaction-status client)
                  _ (core/query! client {:text "ROLLBACK"})
                  s3 (core/transaction-status client)
                  _ (core/query! client {:text "BEGIN"})
                  _ (core/query! client {:text "COMMIT"})
                  s4 (core/transaction-status client)
                  _ (core/end! client)
                  s5 (core/transaction-status client)]
            (is (= [:idle :in-transaction :in-failed-transaction :idle :idle] [s0 s1 s2 s3 s4]))
            (is (= "22012" (:code d)))
            (is (= :idle s5) "the last reported status survives end!")
            (is (= [:pg-cljs/config-error :client] (let [d (thrown-data #(core/transaction-status :nope))]
                                                    [(:type d) (:arg d)])))))))))
