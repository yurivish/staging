(ns toolkit.pubsub-property-test
  "Property-based test for toolkit.pubsub: random interleavings of
   sub/unsub/pub ops; for each pub, assert that the set of handlers
   invoked forms a valid `pick-one` of what a naive matcher would return
   at that moment. Multiplicity-aware — `:fired` is a vector in call
   order so a duplicate-delivery bug can't hide behind set equality."
  (:require [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [toolkit.pubsub :as ps]
            [toolkit.sublist-property-test :as spt]))

(def gen-op
  (gen/one-of
   [(gen/tuple (gen/return :sub)   spt/gen-pattern spt/gen-queue)
    (gen/tuple (gen/return :unsub) gen/nat)
    (gen/tuple (gen/return :pub)   spt/gen-subject)]))

(def gen-ops (gen/vector gen-op 0 40))

(defn- run-ops [ops]
  (let [pubsub    (ps/make)
        subs      (atom [])      ; [{:id :pattern :queue :unsub :dead?}]
        delivered (atom [])]     ; [{:subject :fired [] :snapshot [...]}]
    (doseq [op ops]
      (case (first op)
        :sub   (let [[_ pat q] op
                     id (count @subs)
                     h  (fn [_ _ _]
                          (swap! delivered
                                 update (dec (count @delivered))
                                 update :fired conj id))
                     u  (ps/sub pubsub pat h (when q {:queue q}))]
                 (swap! subs conj {:id id :pattern pat :queue q :unsub u}))
        :unsub (when (seq @subs)
                 (let [i (mod (second op) (count @subs))
                       s (nth @subs i)]
                   (when-not (:dead? s)
                     ((:unsub s))
                     (swap! subs assoc-in [i :dead?] true))))
        :pub   (let [[_ subj] op
                     snap (vec (remove :dead? @subs))]
                 (swap! delivered conj
                        {:subject subj :fired [] :snapshot snap})
                 (ps/pub pubsub subj ::msg))))
    @delivered))

(defn- valid-delivery? [{:keys [subject fired snapshot]}]
  (let [live      (mapv (fn [{:keys [pattern id queue]}] [pattern id queue])
                        snapshot)
        {:keys [plain groups]} (spt/naive-match live subject)
        fired-set (set fired)
        all-ids   (into plain (mapcat val groups))]
    (and
      ;; Multiplicity: no id called more than once per pub.
      (every? #(= 1 %) (vals (frequencies fired)))
      ;; Every plain sub fired.
      (every? fired-set plain)
      ;; Exactly one member per non-empty queue group.
      (every? (fn [[_ members]]
                (or (empty? members)
                    (= 1 (count (filter members fired)))))
              groups)
      ;; No extras.
      (every? all-ids fired))))

(defspec delivery-set-valid 100
  (prop/for-all [ops gen-ops]
    (every? valid-delivery? (run-ops ops))))
