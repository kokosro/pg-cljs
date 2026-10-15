(ns pg-cljs.examples
  "The examples of the README, runnable OFFLINE against a fake pg client:

     npx shadow-cljs compile examples && node target/examples.js

   Each README example will be a named function of this file, kept in
   lockstep with README.md by the examples test; for now the entry point
   only reports the library version."
  (:require [pg-cljs.core :as pg]))

(defn main [& _]
  (println "pg-cljs examples: api-version" pg/api-version))
