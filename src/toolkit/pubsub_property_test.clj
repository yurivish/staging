(ns toolkit.pubsub-property-test
  "Property-based test for toolkit.pubsub: random interleavings of
   sub/unsub/pub ops; for each pub, assert that the set of handlers
   invoked forms a valid `pick-one` of what a naive matcher would return
   at that moment. Multiplicity-aware — `:fired` is a vector in call
   order so a duplicate-delivery bug can't hide behind set equality."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [toolkit.frng :as frng]
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

;; --- frng-driven simulation ---
;;
;; Same invariants, but driven step-by-step so we can (a) check delivery
;; correctness *per-pub* (localizing failures), (b) explicitly verify that
;; unsubbed handlers never fire again, and (c) run longer interleavings
;; than the batch defspec's 40-op cap. On failure, frng/search reports
;; `{:size :seed}` so the case can be replayed via frng/run-once.

(defn- frng-literal [frng]
  (apply str (repeatedly (inc (frng/int-inclusive frng 3))
                         #(char (+ (int \a) (frng/int-inclusive frng 25))))))

(defn- frng-subject [frng]
  (str/join "." (repeatedly (inc (frng/int-inclusive frng 4))
                            #(frng-literal frng))))

(defn- frng-pattern [frng]
  (let [n         (inc (frng/int-inclusive frng 3))
        tok       #(if (zero? (frng/int-inclusive frng 4))
                     "*"
                     (frng-literal frng))
        toks      (vec (repeatedly n tok))
        trailing? (zero? (frng/int-inclusive frng 3))]
    (str/join "." (if trailing? (conj toks ">") toks))))

(defn- frng-queue [frng]
  (when (zero? (frng/int-inclusive frng 3))
    (frng-literal frng)))

(defn- pubsub-sim [frng]
  (let [pubsub   (ps/make)
        ;; Never shrink: kept so we can verify unsubbed handlers stay silent.
        all-subs (atom [])
        weights  (frng/swarm-weights frng [:sub :unsub :pub])]
    (loop []
      (case (frng/weighted frng weights)
        :sub
        (let [pat   (frng-pattern frng)
              q     (frng-queue frng)
              id    (count @all-subs)
              fired (atom 0)
              alive (atom true)
              h     (fn [_ _ _] (swap! fired inc))
              u     (ps/sub pubsub pat h (when q {:queue q}))]
          (swap! all-subs conj {:id id :pattern pat :queue q
                                :unsub u :fired fired :alive? alive}))

        :unsub
        (let [live-idxs (into [] (keep-indexed (fn [i s] (when @(:alive? s) i)))
                              @all-subs)]
          (when (seq live-idxs)
            (let [i (nth live-idxs (frng/index frng live-idxs))
                  s (nth @all-subs i)]
              ((:unsub s))
              (reset! (:alive? s) false))))

        :pub
        (let [subj    (frng-subject frng)
              all     @all-subs
              before  (mapv #(deref (:fired %)) all)]
          (ps/pub pubsub subj ::msg)
          (let [after   (mapv #(deref (:fired %)) all)
                delta   (mapv - after before)
                fired-ids (into #{}
                                (comp (map-indexed (fn [i d] (when (pos? d) i)))
                                      (filter some?))
                                delta)
                live-triples (into []
                                   (comp (filter #(deref (:alive? %)))
                                         (map (fn [{:keys [pattern id queue]}]
                                                [pattern id queue])))
                                   all)
                {:keys [plain groups]} (spt/naive-match live-triples subj)
                expected-all (into plain (mapcat val groups))
                dead-fired   (keep-indexed
                               (fn [i d]
                                 (when (and (pos? d) (not @(:alive? (nth all i))))
                                   i))
                               delta)]
            (assert (every? #(<= % 1) delta)
                    (str "duplicate delivery on " subj " — deltas=" delta))
            (assert (empty? dead-fired)
                    (str "dead sub fired on " subj ": " (vec dead-fired)))
            (assert (every? fired-ids plain)
                    (str "plain miss on " subj ": missing=" (vec (remove fired-ids plain))))
            (assert (every? (fn [[_ members]]
                              (or (empty? members)
                                  (= 1 (count (filter fired-ids members)))))
                            groups)
                    (str "queue group invariant failed on " subj
                         ": fired=" fired-ids " groups=" groups))
            (assert (every? expected-all fired-ids)
                    (str "extras on " subj ": " (vec (remove expected-all fired-ids)))))))
      (recur))))

(defn- run-frng-sim!
  "Run a frng simulation via `frng/search` under clojure.test. On failure,
   include the replay recipe `{:size :seed}` in the assertion message."
  [test-fn opts]
  (let [result (frng/search test-fn opts)]
    (is (= :pass result)
        (when (map? result)
          (let [{:keys [size seed ^Throwable ex]} (:fail result)]
            (str "simulation failed at size=" size " seed=" seed
                 " — replay via (frng/run-once <test-fn> {:size " size
                 " :seed " seed "}) — cause: " (.getMessage ex)))))))

(deftest delivery-invariants-frng
  (run-frng-sim! pubsub-sim {:attempts 30 :max-size 8192}))
