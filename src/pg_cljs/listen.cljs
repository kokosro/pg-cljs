(ns pg-cljs.listen
  "LISTEN/NOTIFY as core.async channels, and server notices the same way.
   A connection subscribes to a channel name with the SQL command LISTEN;
   from then on the server pushes a NotificationResponse for every NOTIFY
   on that name (from any connection, including the subscribed one) at
   any time, between queries or during one, until UNLISTEN or the
   connection ends. `pg` re-emits each as a `notification` event on the
   client; listen! turns that event into a core.async channel.

   Delivery: each channel has a sliding buffer (1024 maps by default, the
   :buffer-size option) filled the moment a notification arrives, whether
   or not anyone is taking from it; when the buffer is full the OLDEST
   notification is dropped to make room for the newest. Nothing is
   replayed: a notification sent while the connection was down, or
   dropped from a full buffer, is gone (that is PostgreSQL's own
   contract too: NOTIFY is not queued across disconnects).

   Which connection: a LISTEN belongs to one physical connection. Use a
   dedicated pg-cljs.core/connect! client, or a client checked out of a
   pool and not released while listening (pg-cljs.pool/query! may run
   each call on a different client, and a released client may be ended
   by the pool, taking the subscription with it)."
  (:require [cljs.core.async :as async]
            [cljs.core.async.impl.protocols :as impl]
            [clojure.string :as str]
            [promesa.core :as p]
            [pg-cljs.binding :as b]
            [pg-cljs.core :as core]
            [pg-cljs.util :as util]))

(def default-buffer-size
  "Notifications (or notices) a channel holds before the oldest is
   dropped: the sliding-buffer size listen! and notices! use when
   :buffer-size is absent."
  1024)

(def listen-options
  "Options of listen! and notices!: :buffer-size, a positive integer
   (default default-buffer-size)."
  #{:buffer-size})

(defn- check-options!
  [op opts]
  (when-not (or (nil? opts) (map? opts))
    (throw (util/config-error (str (name op) "!: options must be a map")
                              {:arg :opts :value opts :op op})))
  (let [unknown (remove #(contains? listen-options %) (keys opts))]
    (when (seq unknown)
      (throw (util/config-error (str (name op) "!: unknown option(s) " (pr-str (vec unknown))
                                     "; allowed: " (pr-str (vec (sort listen-options))))
                                {:arg :opts :unknown (set unknown) :allowed listen-options :op op}))))
  (let [n (get opts :buffer-size default-buffer-size)]
    (when-not (and (integer? n) (pos? n))
      (throw (util/config-error (str (name op) "!: :buffer-size must be a positive integer")
                                {:arg :buffer-size :value n :op op})))
    n))

(defn- sliding-chan [n]
  (async/chan (async/sliding-buffer n)))

;; ---------------------------------------------------------------------------
;; Subscription bookkeeping on the client handle's state atom:
;;   :listens {channel-name [{:chan ch :listener f} ...]}
;;   :notice-listeners [{:chan ch :listener f} ...]
;;   :listen-end-hook f   (one per client: closes everything on `end`)
;; ---------------------------------------------------------------------------

(defn- detach!
  "Removes one upstream listener and closes its channel; drops it from the
   client's bookkeeping under `path`."
  [client path event {:keys [chan listener]}]
  (b/off (:js client) event listener)
  (async/close! chan)
  (swap! (:state client) update-in path
         (fn [subs] (vec (remove #(identical? (:chan %) chan) subs)))))

(defn- close-all!
  "Everything subscribed on the client: called once when the connection
   ends (no notification can arrive any more)."
  [client]
  (let [{:keys [listens notice-listeners]} @(:state client)]
    (doseq [[channel-name subs] listens, sub subs]
      (detach! client [:listens channel-name] "notification" sub))
    (doseq [sub notice-listeners]
      (detach! client [:notice-listeners] "notice" sub))))

(defn- ensure-end-hook!
  [client]
  (when-not (:listen-end-hook @(:state client))
    (let [f (b/once (:js client) "end" (fn [] (close-all! client)))]
      (swap! (:state client) assoc :listen-end-hook f))))

(defn- subscribe!
  "Attaches an upstream listener for `event` feeding `ch` with
   (->data msg) when (keep? data); a channel the consumer closed detaches
   itself at the next event. Records the subscription under `path`."
  [client path event ch ->data keep?]
  (let [sub (atom nil)
        listener (fn [msg]
                   (if (impl/closed? ch)
                     (detach! client path event @sub)
                     (let [m (->data msg)]
                       (when (keep? m)
                         (async/put! ch m)))))]
    (reset! sub {:chan ch :listener listener})
    (b/on (:js client) event listener)
    (swap! (:state client) update-in path (fnil conj []) @sub)
    (ensure-end-hook! client)
    @sub))

(defn- check-name! [op channel-name]
  (when-not (and (string? channel-name) (not (str/blank? channel-name)))
    (throw (util/config-error (str (name op) "!: the channel name must be a non-blank string")
                              {:arg :channel :value channel-name :op op}))))

;; ---------------------------------------------------------------------------
;; Public surface
;; ---------------------------------------------------------------------------

(defn listen!
  "Subscribes the client's connection to notifications on `channel-name`
   and resolves with a core.async channel of them, one map per NOTIFY:
   {:channel \"jobs\" :payload \"...\" :process-id 12345}: :channel the
   name exactly as the server reports it, :payload the NOTIFY payload
   string verbatim (the empty string when NOTIFY had none), :process-id
   the backend process id of the connection that sent it (equal to the
   listening connection's own when it notified itself). The channel name
   is sent quoted as an identifier (LISTEN \"jobs\"), so it is matched
   exactly and case-sensitively: a NOTIFY must spell it the same way
   (NOTIFY \"Jobs\" or pg_notify('Jobs', ...) for a mixed-case name; an
   unquoted NOTIFY Jobs reaches the lower-cased \"jobs\"). Only
   notifications for this name reach this channel; listen! again for
   another name, or again for the same name to get a second channel.

   The upstream listener is attached before LISTEN is sent, so the first
   notification after the subscription is never missed; the promise
   resolves when the server has confirmed LISTEN (its query! result is
   {:command \"LISTEN\" :row-count nil :rows [] ...}; when a NOTIFY to
   this connection coincides with a query, the notification is delivered
   on the channel while the query runs and never appears in query rows).
   The channel has a sliding buffer of :buffer-size (default 1024): it
   fills whether or not a consumer takes, and the OLDEST notification is
   dropped when it is full; what is dropped is gone, there is no replay.
   The channel closes on unlisten! and when the connection ends (end! or
   a broken socket). Closing it yourself (core.async/close!) stops
   delivery and detaches the upstream listener at the next notification,
   but sends no UNLISTEN: the server keeps pushing until unlisten! or
   end!. Options: {:buffer-size n}; an unknown option, a non-positive
   size, a blank name or a non-client are a config-error. Rejects as
   query! does when the server refuses the LISTEN (a query-error, the
   channel is then closed) or the connection is gone (connect-error)."
  ([client channel-name] (listen! client channel-name nil))
  ([client channel-name opts]
   (util/run-op
    identity
    (fn []
      (core/check-client! :listen client)
      (check-name! :listen channel-name)
      (let [n (check-options! :listen opts)
            ch (sliding-chan n)
            sub (subscribe! client [:listens channel-name] "notification" ch
                            b/notification->data
                            (fn [m] (= channel-name (:channel m))))]
        (-> (core/query! client {:text (str "LISTEN " (b/escape-identifier channel-name))})
            (p/then (fn [_] ch))
            (p/catch (fn [e]
                       (detach! client [:listens channel-name] "notification" sub)
                       (throw e)))))))))

(defn unlisten!
  "Sends UNLISTEN for `channel-name` (quoted as listen! quoted it), closes
   every channel listen! opened for that name on this client and detaches
   their upstream listeners; resolves with nil. The channels are closed
   before UNLISTEN is sent, so nothing arrives on them after unlisten! is
   called; the server stops pushing once UNLISTEN completed (its result is
   {:command \"UNLISTEN\" ...}). With no live channel for the name the
   UNLISTEN is still sent (a subscription made by a plain query! is
   undone the same way). Rejects as query! does."
  [client channel-name]
  (util/run-op
   identity
   (fn []
     (core/check-client! :unlisten client)
     (check-name! :unlisten channel-name)
     (doseq [sub (get-in @(:state client) [:listens channel-name])]
       (detach! client [:listens channel-name] "notification" sub))
     (p/then (core/query! client {:text (str "UNLISTEN " (b/escape-identifier channel-name))})
             (fn [_] nil)))))

(defn listening
  "The channel names this client currently has listen! channels open for,
   as a set (synchronous). A LISTEN sent through a plain query! is not
   counted. Throws a config-error for a non-client."
  [client]
  (core/check-client! :listening client)
  (into #{} (keep (fn [[channel-name subs]] (when (seq subs) channel-name)))
        (:listens @(:state client))))

(defn notices!
  "A core.async channel of the server's notices on this connection
   (RAISE NOTICE / WARNING / INFO, a NoticeResponse message): maps of
   {:message \"...\" :severity \"NOTICE\" :code \"00000\"} plus the other
   error-style fields the server sent (:detail :hint :where :position
   :schema :table :column :constraint :file :line :routine ...), exactly
   as a query-error carries them; a notice is not an error and never
   rejects a query. Returned synchronously: no SQL is involved, notices
   flow as soon as the upstream listener is attached. The buffer and drop
   semantics are listen!'s (sliding, :buffer-size, default 1024, oldest
   dropped). The channel closes when the connection ends; closing it
   yourself stops delivery (the upstream listener is detached at the next
   notice). Notices and notifications are separate channels because a
   notice belongs to the connection, not to a LISTEN name. Throws a
   config-error for a non-client or a bad option."
  ([client] (notices! client nil))
  ([client opts]
   (core/check-client! :notices client)
   (let [n (check-options! :notices opts)
         ch (sliding-chan n)]
     (subscribe! client [:notice-listeners] "notice" ch b/notice->data (constantly true))
     ch)))
