(ns pg-cljs.pool
  "Connection pools: a pool is built from the same kebab-case configuration
   as a client (pg-cljs.core/connect!) plus the pool keys (:max :min
   :max-uses :max-lifetime-seconds :idle-timeout-ms :allow-exit-on-idle),
   runs one-off queries on whichever client is free (query!), and hands
   out dedicated clients for transactions or LISTEN (acquire! / release! /
   with-client!). A client checked out of a pool is the SAME handle type as
   a connect! client: pg-cljs.core/query!, transaction-status and
   pg-cljs.listen work on it unchanged.

   Conventions, as in pg-cljs.core:
   - names ending in `!` perform I/O and return a promise; pool! is the
     exception: it allocates the pool (timers, no socket) and returns the
     handle synchronously;
   - a pool handle is opaque (printing shows host, port, database, user,
     :max and the live counters, never the password);
   - rejections use the one pg-cljs taxonomy: :pg-cljs/config-error
     before anything is sent (an unknown key, a wrong argument, a second
     release! of the same checkout), :pg-cljs/connect-error when a client
     cannot be obtained or the pool is gone (refused, timed out, used after
     end!), :pg-cljs/query-error when the server rejects a query;
   - upstream names that would collide with pg-cljs.core when both
     namespaces are referred into one (connect!, query!, end!) have a
     pool-prefixed alias: pool-connect!, pool-query!, pool-end!,
     pool-status;
   - nothing is chosen for the caller: no listener on the pool's `error`
     event. An IDLE pooled connection that breaks is removed by the pool,
     which then emits `error` with the error and that client; a Node
     EventEmitter with no `error` listener throws. Subscribe with
     (pg-cljs.binding/on (:js pool) \"error\" f) when the process must
     survive an idle connection breaking; the same `on` reaches the
     `connect`, `acquire`, `release` and `remove` events."
  (:require [promesa.core :as p]
            [pg-cljs.binding :as b]
            [pg-cljs.core :as core]
            [pg-cljs.util :as util]))

;; ---------------------------------------------------------------------------
;; The handle
;; ---------------------------------------------------------------------------

(defrecord Pool
  ;; js: the upstream pg.Pool. Nothing else: the counters and the ending /
  ;; ended flags are read live off the upstream object.
  [js])

(defn status
  "The live state of the pool, synchronously: {:total-count n (every
   client, idle or checked out) :idle-count n :waiting-count n (acquire!
   calls queued because the pool is full) :expired-count n (clients past
   :max-lifetime-seconds still held) :ending? :ended? :max :min}. The
   counters are upstream's totalCount / idleCount / waitingCount /
   expiredCount at the moment of the call. Throws a config-error for a
   non-pool argument."
  [pool]
  (when-not (instance? Pool pool)
    (throw (util/config-error "status: first argument must be a pool handle"
                              {:arg :pool :value pool :op :status})))
  (merge (select-keys (b/pool-info (:js pool)) [:max :min])
         (b/pool-counts (:js pool))))

(extend-protocol IPrintWithWriter
  Pool
  (-pr-writer [pool w _]
    (let [{:keys [ending? ended?] :as counts} (b/pool-counts (:js pool))
          facts (merge (b/pool-info (:js pool))
                       counts
                       {:status (cond ended? :ended ending? :ending :else :open)})]
      ;; at most eight entries, so the array map keeps this order when printed
      (-write w (str "#pg-cljs/pool"
                     (pr-str (reduce (fn [m k] (if (contains? facts k) (assoc m k (get facts k)) m))
                                     (array-map)
                                     [:host :port :database :user :max :total-count :idle-count :status])))))))

(defn pool?
  "True for a pool handle produced by pool!."
  [x]
  (instance? Pool x))

(defn- check-pool! [op pool]
  (when-not (pool? pool)
    (throw (util/config-error (str (name op) "!: first argument must be a pool handle")
                              {:arg :pool :value pool :op op}))))

(defn- ->connect-error [op e]
  (util/normalize-failure :connect op b/error-info e))

(defn- ->query-failure [op e]
  (util/normalize-failure :query op b/error-info e))

;; ---------------------------------------------------------------------------
;; Construction
;; ---------------------------------------------------------------------------

(defn pool!
  "Creates a pool and returns its handle at once, without connecting:
   upstream connects lazily, one client per concurrent need up to :max,
   and ends idle clients above :min after :idle-timeout-ms. `config` is a
   map of pg-cljs.util/pool-config-keys (every connect! key, handed to
   each client the pool creates, plus :max :min :max-uses
   :max-lifetime-seconds :idle-timeout-ms :allow-exit-on-idle), a
   connection string, or nil / {} (see pg-cljs.core/from-env: `pg` reads
   the PG* variables). Upstream defaults apply to absent keys: :max 10,
   :min 0, :idle-timeout-ms 10000, :max-uses unlimited,
   :max-lifetime-seconds 0 (off), :allow-exit-on-idle false; with
   :connection-timeout-ms absent an acquire! on a full pool waits without
   limit. The one key not offered is the onConnect hook (a JavaScript
   callback run on the raw client before each new client is handed out).
   Throws a config-error synchronously for an unknown key or a wrong
   argument. The idle timers keep the Node process alive until end! (or
   :allow-exit-on-idle true)."
  [config]
  (let [cfg (-> config util/pool-config util/normalize-config util/check-pool-config!)]
    (->Pool (b/make-pool (b/config->js (util/pool-config->upstream cfg))))))

;; ---------------------------------------------------------------------------
;; Checkout and release
;; ---------------------------------------------------------------------------

(defn acquire!
  "Checks a client out of the pool and resolves with its handle (the same
   type as a pg-cljs.core/connect! client, printing :status :connected).
   Upstream order: an idle client is handed out on the next tick, a new
   one is connected when the pool is below :max, otherwise the call queues
   first-in first-out until a release! frees one (or rejects \"timeout
   exceeded when trying to connect\" after :connection-timeout-ms when
   set). The client is the caller's alone until release!: run a
   transaction or a LISTEN on it, then release it, in a p/finally so a
   rejection releases too (with-client! does exactly that). Rejects with
   :pg-cljs/connect-error when no client can be connected (socket error,
   \"Connection terminated due to connection timeout\", a refused startup)
   or the pool has been ended (\"Cannot use a pool after calling end on
   the pool\")."
  [pool]
  (util/run-op
   (partial ->connect-error :acquire)
   (fn []
     (check-pool! :acquire pool)
     (p/then (b/pool-connect (:js pool))
             (fn [js-client]
               (let [handle (core/->Client js-client (atom {:status :connected :pooled? true}))
                     end-listener (b/once js-client "end"
                                          (fn [] (swap! (:state handle) assoc :status :ended)))]
                 (swap! (:state handle) assoc :end-listener end-listener)
                 handle))))))

(defn release!
  "Returns a checked-out client to the pool and resolves with nil. With
   `destroy?` truthy (upstream `release(err)`, `release(true)`: both
   behave the same) the connection is ended and removed from the pool
   instead; the value is passed to upstream verbatim, so an Error reaches
   a `release` event listener as its first argument. Upstream also removes
   the client at release when the pool is ending, when the connection is
   no longer queryable (it broke or was ended), when :max-uses is reached
   or when :max-lifetime-seconds expired. A client is released once: a
   second release! of the same checkout is a config-error (upstream throws
   \"Release called on client which has already been released to the
   pool.\" and pg-cljs refuses before reaching it); a client that was not
   checked out of a pool is a config-error too. After release! the handle
   prints :status :released and must not be used."
  ([client] (release! client nil))
  ([client destroy?]
   (util/run-op
    (partial ->connect-error :release)
    (fn []
      (core/check-client! :release client)
      (let [{:keys [pooled? status end-listener]} @(:state client)]
        (when-not pooled?
          (throw (util/config-error "release!: the client was not checked out of a pool"
                                    {:arg :client :value client :op :release})))
        (when (= :released status)
          (throw (util/config-error "release!: the client has already been released to the pool"
                                    {:arg :client :value client :op :release})))
        (when (some? end-listener)
          (b/off (:js client) "end" end-listener))
        (swap! (:state client) assoc :status :released :end-listener nil)
        (b/pool-release (:js client) (when destroy? destroy?))
        nil)))))

(defn with-client!
  "acquire!, then (f client), then release!, resolving with what `f`
   returned (a promise is awaited). The release happens whether `f`
   resolves, rejects or throws, and the rejection is re-thrown after it:
   a client whose connection broke is removed by the pool at that release
   (upstream checks queryability), so a plain release is always right.
   `f` may release! the client itself (to destroy it, say): with-client!
   then skips its own release. A function rather than a macro: the body
   is an ordinary (fn [client] ...) returning a promise, with no
   compile-time dependency."
  [pool f]
  (p/let [client (acquire! pool)]
    (p/create
     (fn [resolve reject]
       (let [finish (fn [v e]
                      (let [released (if (= :released (:status @(:state client)))
                                       (util/run-op identity (fn [] nil))
                                       (release! client))]
                        (p/handle released
                                  (fn [_ release-error]
                                    (cond
                                      (some? e) (reject e)
                                      (some? release-error) (reject release-error)
                                      :else (resolve v))))))]
         (try
           (let [r (f client)]
             (if (p/promise? r)
               (p/handle r finish)
               (finish r nil)))
           (catch :default e
             (finish nil e))))))))

;; ---------------------------------------------------------------------------
;; Queries
;; ---------------------------------------------------------------------------

(defn query!
  "Runs one command on whichever client is free and resolves with the
   same result map(s) as pg-cljs.core/query!, for the same `cmd` map
   ({:text :values :name :row-mode :types :query-timeout-ms}; the same
   config-errors apply). Upstream checks a client out (connecting one when
   needed), runs the one query, releases it and resolves. When the query
   fails the client is released WITH the error, which removes it from the
   pool: every failed query! costs a reconnect on the next call, and
   :name prepared statements belong to one connection, so they are
   re-parsed on whichever client runs them. Not for transactions: BEGIN
   and COMMIT on successive query! calls may run on different clients;
   use acquire! or with-client!. Rejects as pg-cljs.core/query! does
   (:pg-cljs/query-error for a server rejection or the read timeout,
   :pg-cljs/connect-error when the connection is gone) plus
   :pg-cljs/connect-error when no client can be obtained or the pool has
   been ended (\"Cannot use a pool after calling end on the pool\")."
  [pool cmd]
  (util/run-op
   (partial ->query-failure :query)
   (fn []
     (check-pool! :query pool)
     (let [js-cmd (b/query->js (util/query->upstream (util/check-query! cmd)))]
       (p/then (b/pool-query (:js pool) js-cmd) b/result->data)))))

;; ---------------------------------------------------------------------------
;; Lifecycle
;; ---------------------------------------------------------------------------

(defn connect!
  "Connects the pool's first client eagerly and resolves with the pool
   handle once it is ready: upstream connects only on first use, so this
   is how a caller sequences \"the database is reachable\" before doing
   work, or fails fast. Implemented as acquire! followed by release!: the
   client then sits idle (subject to :idle-timeout-ms like any idle
   client), and a pool that already has clients just checks one out and
   back in. Rejects with :pg-cljs/connect-error exactly as acquire!
   does."
  [pool]
  (p/let [client (acquire! pool)
          _ (release! client)]
    pool))

(defn end!
  "Drains and ends the pool, resolving with nil. Upstream: every idle
   client is ended at once (Terminate sent, socket closed); a checked-out
   client is ended when it is released; the promise resolves once the
   last client has been told to end, which for checked-out clients means
   after their release! and socket close. acquire! calls still queued
   when end! is called are never answered (set :connection-timeout-ms to
   bound them). After end! every acquire! and query! rejects with a
   connect-error (\"Cannot use a pool after calling end on the pool\"),
   and a second end! rejects with a connect-error (\"Called end on pool
   more than once\"): unlike a client's end!, a pool's is not idempotent."
  [pool]
  (util/run-op
   (partial ->connect-error :end)
   (fn []
     (check-pool! :end pool)
     (p/then (b/pool-end (:js pool)) (fn [_] nil)))))

;; ---------------------------------------------------------------------------
;; Aliases for callers referring both pg-cljs.core and pg-cljs.pool
;; ---------------------------------------------------------------------------

(def pool-connect!
  "Alias of connect! (the pool's eager first connection); the name that
   does not collide with pg-cljs.core/connect!."
  connect!)

(def pool-query!
  "Alias of query! (one command on a free pooled client); the name that
   does not collide with pg-cljs.core/query!."
  query!)

(def pool-end!
  "Alias of end! (drain and end the pool); the name that does not collide
   with pg-cljs.core/end!."
  end!)

(def pool-status
  "Alias of status (the live counters of the pool)."
  status)
