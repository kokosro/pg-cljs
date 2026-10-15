(ns pg-cljs.examples-test
  "The examples of the README, run. Network-free: the server is the
   wire-protocol fake of pg-cljs.fake-server, scripted by
   dev/pg_cljs/examples.cljs, and a tripwire stands where a configuration
   without a fake would open a socket.

   The README is held to the code:
   - what stands between a README:begin and a README:end mark of
     dev/pg_cljs/examples.cljs stands in README.md as written;
   - every one-liner of `shown` stands in README.md with the value it
     evaluates to;
   - every result README.md prints is the result the example returns.
   Spacing and commas are not compared."
  (:require ["fs" :as fs]
            [cljs.test :refer [deftest is testing async use-fixtures]]
            [clojure.string :as str]
            [promesa.core :as p]
            [pg-cljs.binding :as b]
            [pg-cljs.core :as pg]
            [pg-cljs.examples :as ex]
            [pg-cljs.listen :as listen]
            [pg-cljs.pool :as pool]
            [pg-cljs.util :as util])
  (:require-macros [pg-cljs.doctest :refer [claims]]))

;; ---------------------------------------------------------------------------
;; No socket is opened here
;; ---------------------------------------------------------------------------

(def ^:private config->js-before (atom nil))

(use-fixtures :once
  {:before (fn []
             (reset! config->js-before b/config->js)
             (ex/no-server!))
   :after (fn []
            (set! b/config->js @config->js-before))})

;; ---------------------------------------------------------------------------
;; The one-liners
;; ---------------------------------------------------------------------------

(def shown
  "The one-liners of the README, each with its value."
  (claims
   (pg/from-env)
   (pg/client? (pg/from-env))
   pg/api-version
   (util/transaction-status "T")
   (sort (keys util/pool-only-keys))
   (util/config->upstream {:application-name "billing" :connection-timeout-ms 5000})
   (util/query->upstream {:text "SELECT $1" :values [1] :row-mode :array})
   (util/unwrap-env-value "\"s3cret-\"")
   (sort util/error-types)
   listen/default-buffer-size))

;; ---------------------------------------------------------------------------
;; Reading the README
;; ---------------------------------------------------------------------------

(defn- slurp [path]
  (fs/readFileSync path "utf8"))

(defn- plain
  "`text` without what is not compared: commas, and any run of whitespace
   as one space."
  [text]
  (-> text
      (str/replace "," "")
      (str/replace #"\s+" " ")
      str/trim))

(defn- region
  "The text between the marks README:begin `label` and README:end `label`
   of the file at `path`, nil when either mark is missing."
  [path label]
  (let [[_ found] (re-find (re-pattern (str "(?s);; README:begin " label "\\n(.*?)\\n\\s*;; README:end " label))
                           (slurp path))]
    found))

(defn- shows?
  "Does README.md show `text`?"
  [text]
  (str/includes? (plain (slurp "README.md")) (plain text)))

(defn- shown-in-readme
  "Asserts README.md prints `value`."
  [value]
  (is (shows? (pr-str value)) (str "README.md does not show:\n" (pr-str value))))

(defn- run
  "Runs the promise of `(f)` as an async test body."
  [done f]
  (-> (f)
      (p/catch (fn [e] (is false (str "unexpected rejection: " (ex-message e) " " (pr-str (ex-data e))))))
      (p/finally (fn [_ _] (done)))))

(def ^:private products-query
  "SELECT id, name, price_cents FROM products WHERE price_cents < $1 ORDER BY id")

;; ---------------------------------------------------------------------------
;; Tests
;; ---------------------------------------------------------------------------

(deftest a-forgotten-fake-fails-at-once
  (async done
    (run done
      (fn []
        (-> (pg/connect! ex/config)
            (p/then (fn [_] (is false "a connection was opened")))
            (p/catch (fn [e]
                       (is (= :pg-cljs.examples/offline (:type (ex-data e)))))))))))

(deftest a-forgotten-fake-fails-a-pool-at-once
  (is (= :pg-cljs.examples/offline
         (:type (ex-data (try (pool/pool! ex/config) (catch :default e e)))))))

(deftest the-quick-start-reads-rows
  (async done
    (run done
      (fn []
        (p/let [r (ex/quick-start-offline)]
          (testing "rows keyed by the exact column names, int4 a number, int8 a string"
            (is (= {:command "SELECT"
                    :row-count 2
                    :rows [{"id" 1 "name" "pencil" "price_cents" "150"}
                           {"id" 2 "name" "notebook" "price_cents" "450"}]
                    :sent {:text products-query :values ["500"] :protocol :extended}}
                   r)))
          (shown-in-readme r))))))

(deftest a-transaction-runs-on-one-pooled-client
  (async done
    (run done
      (fn []
        (p/let [r (ex/pool-offline)]
          (is (= {:transfer {:during :in-transaction :after :idle :stock {"warehouse" 7 "floor" 3}}
                  :status {:total-count 1 :idle-count 1 :waiting-count 0 :max 2}
                  :ended nil
                  :sent ["BEGIN"
                         "UPDATE stock SET warehouse = warehouse - $1 WHERE product_id = $2"
                         "UPDATE stock SET floor = floor + $1 WHERE product_id = $2 RETURNING warehouse, floor"
                         "COMMIT"]}
                 r))
          (shown-in-readme r))))))

(deftest the-transaction-status-follows-the-block
  (async done
    (run done
      (fn []
        (p/let [r (ex/transaction-states-offline)]
          (is (= {:statuses [:idle :in-transaction :in-failed-transaction :idle]
                  :failed {:type :pg-cljs/query-error :code "23505" :constraint "products_pkey"}}
                 r))
          (shown-in-readme r))))))

(deftest a-notify-on-one-connection-arrives-on-the-other
  (async done
    (run done
      (fn []
        (p/let [r (ex/listen-offline)]
          (is (= {:job {:process-id 4243 :channel "jobs" :payload "invoice-42"}
                  :still-listening #{}
                  :sent ["LISTEN \"jobs\"" "SELECT pg_notify($1, $2)" "UNLISTEN \"jobs\""]}
                 r))
          (shown-in-readme r))))))

(deftest failures-are-data
  (async done
    (run done
      (fn []
        (p/let [r (ex/errors-offline)]
          (is (= {:missing-table {:ok? false :why :query
                                  :error {:code "42P01" :severity "ERROR"
                                          :message "relation \"prodcuts\" does not exist" :position "15"}}
                  :duplicate {:ok? false :why :query
                              :error {:code "23505" :severity "ERROR"
                                      :message "duplicate key value violates unique constraint \"products_pkey\""
                                      :detail "Key (id)=(1) already exists."
                                      :table "products" :constraint "products_pkey"}}
                  :not-a-map {:ok? false :why :configuration :error {:arg :cmd}}
                  :unknown-key {:ok? false :why :configuration :error {:arg :config :unknown #{:timeout}}}
                  :after-end {:ok? false :why :connection
                              :error {:message "Client was closed and is not queryable"}}}
                 r))
          (shown-in-readme r))))))

(deftest an-unknown-failure-is-rethrown
  (async done
    (run done
      (fn []
        (-> (ex/outcome #(throw (ex-info "other" {:type :other/error})))
            (p/then (fn [_] (is false "a foreign error was swallowed")))
            (p/catch (fn [e] (is (= :other/error (:type (ex-data e)))))))))))

(deftest the-one-liners-hold
  (let [value (into {} shown)]
    (is (= (count shown) (count value)) "no one-liner is shown twice")
    (is (= {} (value "(pg/from-env)")))
    (is (false? (value "(pg/client? (pg/from-env))")))
    (is (= "0.1.0" (value "pg/api-version")))
    (is (= :in-transaction (value "(util/transaction-status \"T\")")))
    (is (= [:allow-exit-on-idle :idle-timeout-ms :max :max-lifetime-seconds :max-uses :min]
           (value "(sort (keys util/pool-only-keys))")))
    (is (= {"application_name" "billing" "connectionTimeoutMillis" 5000}
           (value "(util/config->upstream {:application-name \"billing\", :connection-timeout-ms 5000})")))
    (is (= {"text" "SELECT $1" "values" [1] "rowMode" "array"}
           (value "(util/query->upstream {:text \"SELECT $1\", :values [1], :row-mode :array})")))
    (is (= "s3cret-" (value "(util/unwrap-env-value \"\\\"s3cret-\\\"\")")))
    (is (= [:pg-cljs/config-error :pg-cljs/connect-error :pg-cljs/query-error :pg-cljs/unsupported-error]
           (value "(sort util/error-types)")))
    (is (= 1024 (value "listen/default-buffer-size")))))

(deftest the-readme-shows-what-runs
  (testing "the marked code of the examples"
    (doseq [label ["config" "quick-start" "going-live" "pool" "transaction-states" "listen" "errors"]
            :let [code (region "dev/pg_cljs/examples.cljs" label)]]
      (is (some? code) (str "no region " label))
      (is (shows? code) (str "README.md does not show " label ":\n" code))))
  (testing "every one-liner, with its value"
    (doseq [[source value] shown
            :let [line (str source "\n;; => " (pr-str value))]]
      (is (shows? line) (str "README.md does not show:\n" line))))
  (testing "how the examples and the smoke are run, and where the manual is"
    (is (shows? "npx shadow-cljs compile examples && node target/examples.js"))
    (is (shows? "npx shadow-cljs compile smoke && node target/smoke.js"))
    (is (shows? "docs/pg-cljs.org"))
    (is (shows? "docs/pg-api-inventory.org"))))
