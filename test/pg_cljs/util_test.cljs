(ns pg-cljs.util-test
  "The pure helpers: key translation tables, config and command validation,
   the error constructors, the environment copy-paste defense."
  (:require [cljs.test :refer [deftest is testing]]
            [pg-cljs.util :as util]))

(defn- thrown-data [f]
  (try (f) nil (catch :default e (ex-data e))))

;; ---------------------------------------------------------------------------
;; Translation tables
;; ---------------------------------------------------------------------------

(deftest the-config-table-spells-upstream-exactly
  (testing "camelCase and snake_case upstream names are both in the table, as pg reads them"
    (is (= {:user "user" :password "password" :host "host" :port "port" :database "database"
            :connection-string "connectionString"
            :ssl "ssl" :ssl-negotiation "sslnegotiation"
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
            :stream "stream"}
           util/client-config-keys)))
  (testing "query command keys"
    (is (= {:text "text" :values "values" :name "name" :row-mode "rowMode"
            :types "types" :query-timeout-ms "query_timeout"}
           util/query-keys)))
  (testing "the fixed-shape envelopes, in upstream order"
    (is (= [:command :row-count :oid] (mapv second util/result-keys)))
    (is (= [:name :table-id :column-id :data-type-id :data-type-size :data-type-modifier :format]
           (mapv second util/field-keys)))
    (is (= ["name" "tableID" "columnID" "dataTypeID" "dataTypeSize" "dataTypeModifier" "format"]
           (mapv first util/field-keys)))
    (is (= [:severity :code :detail :hint :position :internal-position :internal-query :where
            :schema :table :column :data-type :constraint :file :line :routine]
           (mapv second util/error-field-keys)))
    (is (= [:process-id :channel :payload] (mapv second util/notification-keys)))))

;; ---------------------------------------------------------------------------
;; Configuration
;; ---------------------------------------------------------------------------

(deftest connect-config-accepts-nil-string-map
  (is (= {} (util/connect-config nil)))
  (is (= {:connection-string "postgres://u@h/d"} (util/connect-config "postgres://u@h/d")))
  (is (= {:host "h"} (util/connect-config {:host "h"})))
  (let [d (thrown-data #(util/connect-config 42))]
    (is (= :pg-cljs/config-error (:type d)))
    (is (= {:arg :config :value 42 :op :connect} (select-keys d [:arg :value :op])))))

(deftest normalize-config-drops-nils-only
  (is (= {} (util/normalize-config nil)))
  (is (= {:host "h" :port 0 :ssl false} (util/normalize-config {:host "h" :port 0 :ssl false :password nil}))))

(deftest check-config-rejects-unknown-keys-before-anything-is-sent
  (is (= {:host "h"} (util/check-config! {:host "h"})))
  (let [d (thrown-data #(util/check-config! {:host "h" :bogus 1 :connectionTimeoutMillis 5}))]
    (is (= :pg-cljs/config-error (:type d)))
    (is (= #{:bogus :connectionTimeoutMillis} (:unknown d)))
    (is (= (set (keys util/client-config-keys)) (:allowed d)))
    (is (= :config (:arg d))))
  (testing "an :ssl map is checked too"
    (let [d (thrown-data #(util/check-config! {:ssl {:reject-unauthorized false :bogus 1}}))]
      (is (= :pg-cljs/config-error (:type d)))
      (is (= #{:bogus} (:unknown d)))
      (is (= :ssl (:arg d)))))
  (testing "a non-map"
    (is (= :pg-cljs/config-error (:type (thrown-data #(util/check-config! "postgres://")))))))

(deftest config-translation-keeps-values-verbatim
  (let [f (fn [_] :stream)]
    (is (= {"host" "h" "port" 5432 "user" "u" "password" "p@ss\"word" "database" "d"
            "connectionString" "postgres://x" "connectionTimeoutMillis" 300
            "statement_timeout" 1000 "client_encoding" "UTF8" "application_name" "app"
            "keepAlive" true "stream" f
            "ssl" {"rejectUnauthorized" false "ca" "PEM" "servername" "db.example"}}
           (util/config->upstream {:host "h" :port 5432 :user "u" :password "p@ss\"word" :database "d"
                                   :connection-string "postgres://x" :connection-timeout-ms 300
                                   :statement-timeout-ms 1000 :client-encoding "UTF8"
                                   :application-name "app" :keep-alive true :stream f
                                   :ssl {:reject-unauthorized false :ca "PEM" :servername "db.example"}})))
    (is (= {"ssl" true} (util/config->upstream {:ssl true})))
    (is (= {"ssl" "no-verify"} (util/config->upstream {:ssl "no-verify"})))))

;; ---------------------------------------------------------------------------
;; Query commands
;; ---------------------------------------------------------------------------

(deftest check-query-accepts-command-maps-only
  (is (= {:text "SELECT 1"} (util/check-query! {:text "SELECT 1"})))
  (is (= {:text "SELECT $1" :values ["a" 1 nil true [1 2]]}
         (util/check-query! {:text "SELECT $1" :values ["a" 1 nil true [1 2]]})))
  (is (= {:name "q"} (util/check-query! {:name "q"})))
  (testing "a bare string is not a command"
    (let [d (thrown-data #(util/check-query! "SELECT 1"))]
      (is (= :pg-cljs/config-error (:type d)))
      (is (= :cmd (:arg d)))))
  (testing "neither :text nor :name"
    (is (= :cmd (:arg (thrown-data #(util/check-query! {:values [1]}))))))
  (testing "an unknown key"
    (is (= #{:rowMode} (:unknown (thrown-data #(util/check-query! {:text "x" :rowMode "array"}))))))
  (testing ":text not a string"
    (is (= :text (:arg (thrown-data #(util/check-query! {:text 1}))))))
  (testing ":values not a vector"
    (is (= :values (:arg (thrown-data #(util/check-query! {:text "x" :values "nope"}))))))
  (testing "a ClojureScript map among the values"
    (let [d (thrown-data #(util/check-query! {:text "x" :values [1 {:a 1}]}))]
      (is (= :values (:arg d)))
      (is (= 1 (:index d)))))
  (testing ":row-mode"
    (is (= {:text "x" :row-mode :array} (util/check-query! {:text "x" :row-mode :array})))
    (let [d (thrown-data #(util/check-query! {:text "x" :row-mode :object}))]
      (is (= :row-mode (:arg d)))
      (is (= #{:array} (:allowed d))))))

(deftest query-translation
  (is (= {"text" "SELECT $1" "values" ["a"] "name" "q" "rowMode" "array" "query_timeout" 50}
         (util/query->upstream {:text "SELECT $1" :values ["a"] :name "q" :row-mode :array :query-timeout-ms 50})))
  (is (= {"text" "x"} (util/query->upstream {:text "x"}))))

(deftest query-failures-are-classified
  (is (= :query (util/query-failure-kind {:database-error? true :code "42P01" :message "relation missing"})))
  (is (= :query (util/query-failure-kind {:message "Query read timeout"})))
  (is (= :query (util/query-failure-kind {:message "Query values must be an array"})))
  (is (= :connect (util/query-failure-kind {:message "Client was closed and is not queryable"})))
  (is (= :connect (util/query-failure-kind {:message "Connection terminated"})))
  (is (= :connect (util/query-failure-kind {:message "Connection terminated unexpectedly"})))
  (is (= :connect (util/query-failure-kind {:code "ECONNRESET" :message "read ECONNRESET"}))))

;; ---------------------------------------------------------------------------
;; Errors
;; ---------------------------------------------------------------------------

(deftest error-constructors-build-one-taxonomy
  (is (= #{:pg-cljs/config-error :pg-cljs/connect-error :pg-cljs/query-error :pg-cljs/unsupported-error}
         util/error-types))
  (let [e (util/query-error "relation \"t\" does not exist" {:code "42P01" :op :query})]
    (is (= "pg-cljs: relation \"t\" does not exist" (ex-message e)))
    (is (= {:type :pg-cljs/query-error :message "relation \"t\" does not exist" :code "42P01" :op :query}
           (ex-data e)))
    (is (= :pg-cljs/query-error (util/error-type e)))
    (is (= (ex-data e) (util/error-data e))))
  (is (= :pg-cljs/config-error (util/error-type (util/config-error "bad"))))
  (is (= :pg-cljs/connect-error (util/error-type (util/connect-error "refused" {:code "ECONNREFUSED"}))))
  (is (= :pg-cljs/unsupported-error (util/error-type (util/unsupported-error "browser"))))
  (testing "foreign errors are not pg-cljs errors"
    (is (nil? (util/error-type (ex-info "other" {:type :other/error}))))
    (is (nil? (util/error-data (js/Error. "plain"))))
    (is (nil? (util/error-type nil)))))

;; ---------------------------------------------------------------------------
;; Environment values
;; ---------------------------------------------------------------------------

(deftest unwrap-env-value-strips-one-layer
  (is (= "secret" (util/unwrap-env-value "\"secret\"")))
  (is (= "secret" (util/unwrap-env-value "'secret'")))
  (is (= "secret" (util/unwrap-env-value "`secret`")))
  (is (= "secret" (util/unwrap-env-value "-secret-")))
  (is (= "secret" (util/unwrap-env-value "  \"secret\" \n")))
  (testing "exactly one layer"
    (is (= "\"secret\"" (util/unwrap-env-value "\"\"secret\"\"")))
    (is (= "'secret'" (util/unwrap-env-value "\"'secret'\""))))
  (testing "nothing else is removed"
    (is (= "p@ss!word-" (util/unwrap-env-value "p@ss!word-")))
    (is (= "a-b" (util/unwrap-env-value "a-b")))
    (is (= "postgres://u:p@h/d" (util/unwrap-env-value "postgres://u:p@h/d")))
    (is (= "\"" (util/unwrap-env-value "\"")))
    (is (= "\"\"" (util/unwrap-env-value "\"\"")))
    (is (= "\"" (util/unwrap-env-value "\"\"\""))))
  (testing "blank and non-strings are nil"
    (is (nil? (util/unwrap-env-value "")))
    (is (nil? (util/unwrap-env-value "   ")))
    (is (nil? (util/unwrap-env-value nil)))
    (is (nil? (util/unwrap-env-value 42)))))
