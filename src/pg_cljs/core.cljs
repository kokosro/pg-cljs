(ns pg-cljs.core
  "The pg-cljs client: construction of PostgreSQL clients and pools from a
   kebab-case configuration map or from the PG* environment variables, and
   their lifecycle (connect, query, end). The public surface is plain
   ClojureScript data: configuration and query results are maps of
   kebab-cased keywords, asynchronous operations return promesa promises,
   and notifications arrive on core.async channels. No JS interop appears
   at a call site; the `pg` (node-postgres) objects stay inside the single
   binding namespace.")

(def api-version
  "The library's own version marker, as a string. It names the version of
   the pg-cljs public surface, not the version of the wrapped `pg` package."
  "0.1.0")
