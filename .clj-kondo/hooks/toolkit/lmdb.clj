(ns hooks.toolkit.lmdb
  (:require [clj-kondo.hooks-api :as api]))

(defn- bind-first-symbol
  "Shared hook body for macros shaped like `(m [sym & exprs] body...)`.
   Rewrites to `(let [sym (do exprs...)] body...)` so clj-kondo sees
   `sym` as bound and still lints each expr."
  [{:keys [node]}]
  (let [[_ binding & body] (:children node)
        [sym & exprs]      (:children binding)
        new (api/list-node
              (list*
                (api/token-node 'let)
                (api/vector-node
                  [sym (api/list-node
                         (cons (api/token-node 'do) exprs))])
                body))]
    {:node new}))

(def with-txn
  "`(with-txn [sym env mode] body...)` → `(let [sym (do env mode)] body...)`."
  bind-first-symbol)

(def with-env
  "`(with-env [sym opts] body...)` → `(let [sym (do opts)] body...)`."
  bind-first-symbol)
