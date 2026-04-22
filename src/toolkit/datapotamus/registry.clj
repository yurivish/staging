(ns toolkit.datapotamus.registry)

(set! *warn-on-reflection* true)

(defn make [] (atom {}))

(defn install!
  "Install entry under slug. Returns prior entry (or nil). Caller stops any prior."
  [reg slug entry]
  (get (first (swap-vals! reg assoc slug entry)) slug))

(defn current [reg slug] (get @reg slug))

(defn remove-if-matches!
  "Dissoc slug iff its :run-id still matches; avoids clobbering a successor."
  [reg slug run-id]
  (swap! reg (fn [m] (if (= run-id (get-in m [slug :run-id])) (dissoc m slug) m))))
