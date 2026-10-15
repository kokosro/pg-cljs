(ns pg-cljs.api-index-test
  "The API reference of the manual (docs/pg-cljs.org) and the public vars
   of the source agree in both directions: every public var of
   pg-cljs.core, pg-cljs.pool, pg-cljs.listen and pg-cljs.util has a
   reference entry, and every reference entry names a var that exists.
   An entry is a level-four org headline `**** pg-cljs.<ns>/<name>` under
   the reference section; the public vars are read from the compiler's
   analysis at compile time (pg-cljs.doctest/public-vars)."
  (:require ["fs" :as fs]
            [cljs.test :refer [deftest is testing]]
            [clojure.set :as set]
            [clojure.string :as str]
            [pg-cljs.core]
            [pg-cljs.listen]
            [pg-cljs.pool]
            [pg-cljs.util])
  (:require-macros [pg-cljs.doctest :refer [public-vars]]))

(def manual "docs/pg-cljs.org")

(def in-source
  "Namespace name -> the set of its public var names, from the source."
  {"pg-cljs.core" (set (public-vars 'pg-cljs.core))
   "pg-cljs.pool" (set (public-vars 'pg-cljs.pool))
   "pg-cljs.listen" (set (public-vars 'pg-cljs.listen))
   "pg-cljs.util" (set (public-vars 'pg-cljs.util))})

(defn- documented
  "Namespace name -> the set of var names the manual's reference entries
   name (`**** pg-cljs.<ns>/<name>` headlines)."
  [text]
  (reduce (fn [m [_ ns-name var-name]]
            (update m ns-name (fnil conj #{}) var-name))
          {}
          (re-seq #"(?m)^\*\*\*\* (pg-cljs\.[a-z]+)/(\S+)" text)))

(deftest the-manual-indexes-every-public-var-and-nothing-else
  (let [text (fs/readFileSync manual "utf8")
        in-manual (documented text)]
    (testing "the four namespaces have reference entries"
      (is (= (set (keys in-source)) (set (keys in-manual)))))
    (doseq [[ns-name source-vars] in-source
            :let [manual-vars (get in-manual ns-name #{})]]
      (testing (str ns-name ": every public var has an entry")
        (is (empty? (set/difference source-vars manual-vars))
            (str ns-name " public vars without a reference entry: "
                 (pr-str (sort (set/difference source-vars manual-vars))))))
      (testing (str ns-name ": every entry names an existing public var")
        (is (empty? (set/difference manual-vars source-vars))
            (str ns-name " reference entries without a public var: "
                 (pr-str (sort (set/difference manual-vars source-vars)))))))
    (testing "the source has the public surface the README documents"
      (is (contains? (in-source "pg-cljs.core") "connect!"))
      (is (contains? (in-source "pg-cljs.pool") "with-client!"))
      (is (contains? (in-source "pg-cljs.listen") "listen!"))
      (is (not (contains? (in-source "pg-cljs.listen") "check-options!")) "private vars are not public"))
    (testing "the manual points at the run commands"
      (is (str/includes? text "npx shadow-cljs compile examples && node target/examples.js"))
      (is (str/includes? text "npx shadow-cljs compile smoke && node target/smoke.js")))))
