(ns pg-cljs.fake-server
  "A table-driven fake of a PostgreSQL SERVER for the offline suite, at the
   wire level: a Node Duplex stream that behaves like the net.Socket `pg`
   expects, handed to the REAL pg Client through its `stream` config
   option. The real Client runs the whole protocol (startup, simple and
   extended queries, Terminate) against it; the fake parses the frontend
   messages it receives, records every query (text, values, name,
   protocol) and answers from a script with backend messages:

   - startup -> AuthenticationOk (R), BackendKeyData (K), ParameterStatus
     (S), ReadyForQuery (Z);
   - a simple query (Q) -> RowDescription (T), DataRow (D)*,
     CommandComplete (C), Z — one T/D/C group per scripted result for a
     multi-statement text; or ErrorResponse (E) then Z;
   - the extended sequence (P B D E S) -> ParseComplete (1, only when a
     Parse was sent), BindComplete (2), T or NoData (n), D*, C, Z; or E
     then Z;
   - NotificationResponse (A) and NoticeResponse (N) pushed during a query
     when the script asks, or at any time through notify! / notice!;
   - Terminate (X) -> the stream closes.

   Every answer is pushed asynchronously (setImmediate, or after
   :delay-ms), as a socket would deliver it: the Client desynchronizes when
   a response arrives inside its own write call.

   Script: {text spec, ... :default spec}. A spec is
   - a result map {:fields [{:name \"id\" :data-type-id 23} ...]
                   :rows [[\"1\" \"ann\"] ...]      ; wire text per cell, nil = NULL
                   :command \"SELECT 2\"            ; the command tag; default SELECT n
                   :status \"I\"                    ; the ReadyForQuery status byte
                   :notifications [{:process-id 1 :channel \"c\" :payload \"p\"}]
                   :notify-all [{:process-id 1 :channel \"c\" :payload \"p\"}] ; pushed on EVERY live connection, as a server delivers a NOTIFY to every connection that LISTENs
                   :notices [{:severity \"NOTICE\" :code \"00000\" :message \"hi\"}]
                   :delay-ms 50                    ; answer later than next tick
                   :silent? true}                  ; never answer (timeouts)
   - an error map {:error {:severity \"ERROR\" :code \"42P01\" :message \"...\"
                           :detail ... :hint ... :position ... :schema ...}}
   - a vector of result maps: a multi-statement answer (simple protocol).
   A field is a map with :name and optional :table-id :column-id
   :data-type-id (default 25, text) :data-type-size :data-type-modifier
   :format (0 text / 1 binary), or just the name string.
   Unscripted text is answered with an ErrorResponse (code XX000) naming
   it, so a test cannot pass on an accidental default.

   Use: (let [server (fake-server {:script {...}})]
          (connect! {:host \"fake\" :port 5432 :user \"u\" :database \"d\"
                     :stream (stream-option server)}) ...
          (queries server))"
  (:require [clojure.string :as str]))

(def ^:private node-stream (js/require "stream"))

;; ---------------------------------------------------------------------------
;; Backend message encoding
;; ---------------------------------------------------------------------------

(defn- buf-concat [bufs]
  (js/Buffer.concat (to-array bufs)))

(defn- bytes-of [s]
  (if (instance? js/Buffer s) s (js/Buffer.from (str s) "utf8")))

(defn- cstring [s]
  (buf-concat [(bytes-of s) (js/Buffer.from #js [0])]))

(defn- int16 [n]
  (let [b (js/Buffer.alloc 2)] (.writeInt16BE b n 0) b))

(defn- int32 [n]
  (let [b (js/Buffer.alloc 4)] (.writeInt32BE b n 0) b))

(defn- message
  "A typed backend message: one type byte, int32 length (self included,
   type excluded), body."
  [type-char & parts]
  (let [body (buf-concat parts)
        head (js/Buffer.alloc 5)]
    (aset head 0 (.charCodeAt type-char 0))
    (.writeInt32BE head (+ 4 (.-length body)) 1)
    (buf-concat [head body])))

(defn authentication-ok [] (message "R" (int32 0)))
(defn backend-key-data [pid secret] (message "K" (int32 pid) (int32 secret)))
(defn parameter-status [k v] (message "S" (cstring k) (cstring v)))
(defn ready-for-query [status] (message "Z" (js/Buffer.from (str status) "ascii")))
(defn parse-complete [] (message "1"))
(defn bind-complete [] (message "2"))
(defn no-data [] (message "n"))
(defn command-complete [tag] (message "C" (cstring tag)))

(defn- field-spec [f]
  (let [f (if (string? f) {:name f} f)]
    (merge {:table-id 0 :column-id 0 :data-type-id 25 :data-type-size -1
            :data-type-modifier -1 :format 0}
           f)))

(defn row-description [fields]
  (apply message "T" (int16 (count fields))
         (mapcat (fn [f]
                   (let [{:keys [name table-id column-id data-type-id data-type-size
                                 data-type-modifier format]} (field-spec f)]
                     [(cstring name) (int32 table-id) (int16 column-id) (int32 data-type-id)
                      (int16 data-type-size) (int32 data-type-modifier) (int16 format)]))
                 fields)))

(defn data-row [cells]
  (apply message "D" (int16 (count cells))
         (mapcat (fn [cell]
                   (if (nil? cell)
                     [(int32 -1)]
                     (let [b (bytes-of cell)]
                       [(int32 (.-length b)) b])))
                 cells)))

(def ^:private error-letters
  [[:severity "S"] [:severity "V"] [:code "C"] [:message "M"] [:detail "D"] [:hint "H"]
   [:position "P"] [:internal-position "p"] [:internal-query "q"] [:where "W"]
   [:schema "s"] [:table "t"] [:column "c"] [:data-type "d"] [:constraint "n"]
   [:file "F"] [:line "L"] [:routine "R"]])

(defn- error-body [fields]
  (buf-concat
   (concat (mapcat (fn [[k letter]]
                     (when-some [v (get fields k)]
                       [(js/Buffer.from letter "ascii") (cstring v)]))
                   error-letters)
           [(js/Buffer.from #js [0])])))

(defn error-response [fields] (message "E" (error-body fields)))
(defn notice-response [fields] (message "N" (error-body fields)))

(defn notification-response [{:keys [process-id channel payload]}]
  (message "A" (int32 (or process-id 0)) (cstring channel) (cstring (or payload ""))))

;; ---------------------------------------------------------------------------
;; Frontend message decoding
;; ---------------------------------------------------------------------------

(defn- read-cstring
  "[string next-offset] of the NUL-terminated string at `offset`."
  [buf offset]
  (let [end (.indexOf buf 0 offset)]
    [(.toString buf "utf8" offset end) (inc end)]))

(defn- read-cstrings
  "Every cstring from `offset` to `end` (exclusive) as a vector."
  [buf offset end]
  (loop [o offset acc []]
    (if (>= o end)
      acc
      (let [[s n] (read-cstring buf o)]
        (recur n (conj acc s))))))

(defn- decode-startup
  "{:protocol n :parameters {name value}} of a startup body."
  [^js body]
  (let [protocol (.readInt32BE body 0)
        strs (read-cstrings body 4 (.-length body))
        strs (if (= "" (peek strs)) (pop strs) strs)]
    {:protocol protocol
     :parameters (into {} (map vec) (partition 2 strs))}))

(defn- decode-parse [body]
  (let [[name n] (read-cstring body 0)
        [text _] (read-cstring body n)]
    {:name name :text text}))

(defn- decode-bind
  "{:portal :statement :values} of a Bind body; a parameter sent in the
   binary format stays a Buffer, text ones are strings, NULL is nil."
  [^js body]
  (let [[portal o1] (read-cstring body 0)
        [statement o2] (read-cstring body o1)
        nformats (.readInt16BE body o2)
        formats (mapv #(.readInt16BE body (+ o2 2 (* 2 %))) (range nformats))
        o3 (+ o2 2 (* 2 nformats))
        nparams (.readInt16BE body o3)
        [values _] (reduce (fn [[vals o] i]
                             (let [len (.readInt32BE body o)]
                               (if (= -1 len)
                                 [(conj vals nil) (+ o 4)]
                                 (let [slice (.subarray body (+ o 4) (+ o 4 len))
                                       binary? (= 1 (get formats i (get formats 0 0)))]
                                   [(conj vals (if binary? (js/Buffer.from slice) (.toString slice "utf8")))
                                    (+ o 4 len)]))))
                           [[] (+ o3 2)]
                           (range nparams))]
    {:portal portal :statement statement :values values}))

;; ---------------------------------------------------------------------------
;; Answering
;; ---------------------------------------------------------------------------

(defn- normalize-spec [spec]
  (cond
    (vector? spec) {:results (mapv normalize-spec spec) :multi? true}
    (:error spec) spec
    :else (let [rows (vec (:rows spec))]
            (assoc spec
                   :rows rows
                   :fields (vec (:fields spec))
                   :command (or (:command spec) (str "SELECT " (count rows)))))))

(defn- unscripted [text]
  {:error {:severity "ERROR" :code "XX000"
           :message (str "fake-server: no response scripted for: " text)}})

(defn- result-messages
  "The T/D*/C messages of one result map, with its notices and
   notifications first."
  [{:keys [fields rows command notices notifications]} extended?]
  (concat (map notice-response notices)
          (map notification-response notifications)
          (cond
            (seq fields) [(row-description fields)]
            extended? [(no-data)]
            :else [])
          (map data-row rows)
          [(command-complete command)]))

(defn- answer-messages
  "The whole backend answer to a query: a seq of Buffers."
  [spec {:keys [parsed? extended?]}]
  (let [spec (normalize-spec spec)
        prefix (when extended?
                 (concat (when parsed? [(parse-complete)]) [(bind-complete)]))
        status (or (:status spec) "I")]
    (cond
      (:error spec) (concat prefix [(error-response (:error spec)) (ready-for-query (or (:status spec) "I"))])
      (:multi? spec) (concat prefix
                             (mapcat #(result-messages % extended?) (:results spec))
                             [(ready-for-query status)])
      :else (concat prefix (result-messages spec extended?) [(ready-for-query status)]))))

(defn- destroyed? [stream]
  (boolean (unchecked-get stream "destroyed")))

(defn- push-later!
  [stream buf delay-ms]
  (let [go (fn [] (when-not (destroyed? stream) (js-invoke stream "push" buf)))]
    (if delay-ms
      (js/setTimeout go delay-ms)
      (js/setImmediate go))))

;; ---------------------------------------------------------------------------
;; The fake socket
;; ---------------------------------------------------------------------------

(declare notify!)

(defn- broadcast!
  "The :notify-all notifications of a spec, pushed on every live
   connection of the server (the querying one included), after the answer."
  [server spec]
  (doseq [n (:notify-all spec)]
    (notify! server n)))

(defn- handle-message!
  "Dispatches one decoded frontend message."
  [server stream conn code body]
  (let [{:keys [responder]} server]
    (case code
      ;; Q: simple query
      81 (let [[text _] (read-cstring body 0)
               q {:text text :values nil :name nil :protocol :simple}
               spec (responder q)]
           (swap! (:queries server) conj q)
           (when-not (:silent? spec)
             (push-later! stream (buf-concat (answer-messages spec {:extended? false}))
                          (:delay-ms spec))
             (broadcast! server spec)))
      ;; P: parse
      80 (let [{:keys [name text]} (decode-parse body)]
           (swap! conn assoc-in [:pending :parse] {:name name :text text})
           (swap! conn assoc-in [:statements name] text))
      ;; B: bind
      66 (swap! conn assoc-in [:pending :bind] (decode-bind body))
      ;; D: describe, E: execute, H: flush, C: close — nothing to answer by themselves
      (68 69 72 67) nil
      ;; S: sync — answer the whole extended sequence
      83 (let [{:keys [pending statements]} @conn
               {:keys [parse bind]} pending
               stmt (:statement bind)
               q {:text (or (:text parse) (get statements stmt))
                  :values (:values bind)
                  :name (when-not (str/blank? stmt) stmt)
                  :protocol :extended}
               spec (responder q)]
           (swap! conn assoc :pending {})
           (swap! (:queries server) conj q)
           (when-not (:silent? spec)
             (push-later! stream (buf-concat (answer-messages spec {:extended? true :parsed? (some? parse)}))
                          (:delay-ms spec))
             (broadcast! server spec)))
      ;; X: terminate
      88 (do (swap! (:terminated server) inc)
             (js/setImmediate (fn []
                                (when-not (destroyed? stream)
                                  (js-invoke stream "push" nil)
                                  (js-invoke stream "destroy")))))
      (swap! (:unhandled server) conj code))))

(defn- consume!
  "Parses every complete frontend message in the connection's inbound
   buffer; the first message of a connection is the untyped startup."
  [server stream conn]
  (loop []
    (let [^js buf (:inbound @conn)
          len (.-length buf)]
      (if (:startup? @conn)
        (when (>= len 4)
          (let [mlen (.readInt32BE buf 0)]
            (when (>= len mlen)
              (let [body (.subarray buf 4 mlen)
                    {:keys [protocol parameters]} (decode-startup body)]
                (swap! conn assoc :inbound (js/Buffer.from (.subarray buf mlen)) :startup? false)
                (if (= 80877103 protocol)
                  ;; SSLRequest: this fake speaks plain text only
                  (push-later! stream (js/Buffer.from "N" "ascii") nil)
                  (let [{:keys [silent? delay-ms parameters-sent]} (:startup server)]
                    (swap! (:startups server) conj parameters)
                    (when-not silent?
                      (push-later! stream
                                   (buf-concat (concat [(authentication-ok)
                                                        (backend-key-data (:process-id server) (:secret-key server))]
                                                       (map (fn [[k v]] (parameter-status k v))
                                                            (or parameters-sent {"server_version" "16.4"
                                                                                 "client_encoding" "UTF8"}))
                                                       [(ready-for-query "I")]))
                                   delay-ms))))
                (recur)))))
        (when (>= len 5)
          (let [code (aget buf 0)
                mlen (.readInt32BE buf 1)]
            (when (>= len (+ 1 mlen))
              (let [body (.subarray buf 5 (+ 1 mlen))]
                (swap! conn assoc :inbound (js/Buffer.from (.subarray buf (+ 1 mlen))))
                (handle-message! server stream conn code body)
                (recur)))))))))

(defn- make-stream
  "One fake socket for one connection attempt."
  [server js-config]
  (let [conn (atom {:inbound (js/Buffer.alloc 0) :startup? true :pending {} :statements {}})
        holder (atom nil)
        Duplex (.-Duplex node-stream)
        stream (Duplex. #js {:read (fn [_] nil)
                             :write (fn [chunk _ cb]
                                      (swap! conn update :inbound #(buf-concat [% chunk]))
                                      (consume! server @holder conn)
                                      (cb))
                             :final (fn [cb]
                                      (when-not (destroyed? @holder) (js-invoke @holder "push" nil))
                                      (cb))})]
    (reset! holder stream)
    (swap! (:streams server) conj stream)
    (swap! (:configs server) conj js-config)
    (unchecked-set stream "setNoDelay" (fn [_] stream))
    (unchecked-set stream "setKeepAlive" (fn [_ _] stream))
    (unchecked-set stream "ref" (fn [] stream))
    (unchecked-set stream "unref" (fn [] stream))
    (unchecked-set stream "connect"
                   (fn [port host]
                     (swap! (:connects server) conj {:port port :host host})
                     (when-let [ms (:hold-ms server)]
                       ;; the Client's connection timer is unref'd: keep the
                       ;; event loop alive long enough for it to fire
                       (js/setTimeout (fn []) ms))
                     (js/setImmediate (fn [] (js-invoke stream "emit" "connect")))
                     stream))
    stream))

;; ---------------------------------------------------------------------------
;; Public surface
;; ---------------------------------------------------------------------------

(defn fake-server
  "A fake server. opts:
   :script       {text spec ... :default spec} (see the namespace doc)
   :responder    (fn [query] spec) instead of / before the script lookup
   :startup      {:silent? bool :delay-ms n :parameters-sent {name value}}
   :process-id   the BackendKeyData pid (default 4242), :secret-key (default 7)
   :hold-ms      keep the event loop alive this long after each connect
   Returns a map of atoms; read it with queries / startups / connects."
  [opts]
  (let [script (:script opts {})
        responder (or (:responder opts)
                      (fn [q] (get script (:text q) (get script :default (unscripted (:text q))))))]
    {:responder responder
     :startup (:startup opts {})
     :process-id (:process-id opts 4242)
     :secret-key (:secret-key opts 7)
     :hold-ms (:hold-ms opts)
     :queries (atom [])
     :startups (atom [])
     :connects (atom [])
     :configs (atom [])
     :streams (atom [])
     :terminated (atom 0)
     :unhandled (atom [])}))

(defn stream-option
  "The value to put under :stream in a connect! config: a factory the
   Client calls with its connection config, returning a fresh fake socket."
  [server]
  (fn [js-config] (make-stream server js-config)))

(defn queries
  "Every query the fake received, in order: {:text :values :name
   :protocol} (:values are the wire parameters: strings, nil for NULL,
   Buffers for binary; :protocol :simple or :extended)."
  [server]
  @(:queries server))

(defn startups
  "The startup parameter maps received ({\"user\" .. \"database\" ..
   \"client_encoding\" ..}), one per connection."
  [server]
  @(:startups server))

(defn connects
  "The {:port :host} pairs the Client connected its socket to."
  [server]
  @(:connects server))

(defn terminated-count
  "How many Terminate messages arrived."
  [server]
  @(:terminated server))

(defn- live-streams [server]
  (remove destroyed? @(:streams server)))

(defn notify!
  "Pushes a NotificationResponse {:process-id :channel :payload} on every
   live connection, asynchronously."
  [server notification]
  (doseq [s (live-streams server)]
    (push-later! s (notification-response notification) nil))
  nil)

(defn notice!
  "Pushes a NoticeResponse (same fields as an error) on every live
   connection, asynchronously."
  [server fields]
  (doseq [s (live-streams server)]
    (push-later! s (notice-response fields) nil))
  nil)

(defn drop-connections!
  "Destroys every live fake socket, as a server crash would."
  [server]
  (doseq [s (live-streams server)]
    (js-invoke s "destroy"))
  nil)
