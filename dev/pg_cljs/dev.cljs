(ns pg-cljs.dev
  "Node-script entry point for interactive development."
  (:require [pg-cljs.core :as pg]))

(defn main [& _]
  (println "pg-cljs dev runtime loaded. Attach with: npx shadow-cljs cljs-repl dev"
           "api-version:" pg/api-version)
  (js/setInterval #(println "…dev heartbeat") 60000))
