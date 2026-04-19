(ns toolkit.sublist
  "NATS-style subject routing trie with `*` (single-token) and `>` (tail)
   wildcards and queue groups.

   Subscriptions are value-deduped on (subject, value, queue) — inserting
   the same triple twice leaves one copy. A pubsub layer that wants Go-style
   multi-delivery from a single `sub` call should wrap its stored value with
   a unique token (e.g. a gensym inside a map field). That stays in the
   pubsub layer; the sublist stays generic.

   `match` returns {:plain #{value ...} :groups {queue #{value ...}}}. The
   full structure (not a pre-picked delivery set) is load-bearing for
   debug/tracing subscribers that want to see every match. Callers that
   only care about the delivery set use `pick-one`."
  (:require [clojure.string :as str])
  (:import [clojure.lang ExceptionInfo]))

(def ^:private empty-node
  {:psubs #{} :qsubs {} :children {} :pwc nil :fwc nil})

(def ^:private empty-fwc
  {:psubs #{} :qsubs {}})

;; --- validation ---

(defn- bad [reason s & {:as extra}]
  (throw (ex-info (name reason) (merge {:reason reason :subject s} extra))))

(defn- validate-pattern [s]
  (when (or (nil? s) (= s ""))
    (bad :empty-subject s))
  (let [toks (str/split s #"\." -1)
        n    (count toks)]
    (dotimes [i n]
      (let [t (nth toks i)]
        (cond
          (= t "")
          (bad :empty-token s :index i)

          (re-find #"[\s\p{Cntrl}]" t)
          (bad :whitespace-in-token s :index i)

          (and (> (count t) 1)
               (or (str/includes? t "*") (str/includes? t ">")))
          (bad :bad-wildcard s :index i)

          (and (= t ">") (not= i (dec n)))
          (bad :fwc-not-final s :index i))))
    toks))

(defn- validate-subject [s]
  (let [toks (validate-pattern s)]
    (when (some #{"*" ">"} toks)
      (bad :wildcard-in-subject s))
    toks))

(defn valid-pattern? [s]
  (try (validate-pattern s) true (catch ExceptionInfo _ false)))

(defn valid-subject? [s]
  (try (validate-subject s) true (catch ExceptionInfo _ false)))

;; --- insert ---

(defn- add-to-bucket [bucket q v]
  (if q
    (update-in bucket [:qsubs q] (fnil conj #{}) v)
    (update bucket :psubs conj v)))

(defn- ins [node [t & rst] v q]
  (cond
    (nil? t)  (add-to-bucket node q v)
    (= t "*") (update node :pwc #(ins (or % empty-node) rst v q))
    (= t ">") (update node :fwc #(add-to-bucket (or % empty-fwc) q v))
    :else     (update-in node [:children t] #(ins (or % empty-node) rst v q))))

;; --- remove with pruning ---

(defn- disj-from-bucket [bucket q v]
  (if q
    (let [grp  (get-in bucket [:qsubs q] #{})
          grp' (disj grp v)
          qs   (:qsubs bucket)
          qs'  (if (empty? grp') (dissoc qs q) (assoc qs q grp'))]
      (assoc bucket :qsubs qs'))
    (update bucket :psubs disj v)))

(defn- dead? [node]
  (and (empty? (:psubs node))
       (empty? (:qsubs node))
       (empty? (:children node))
       (nil? (:pwc node))
       (nil? (:fwc node))))

(defn- fwc-dead? [fwc]
  (and (empty? (:psubs fwc)) (empty? (:qsubs fwc))))

(defn- rm [node [t & rst] v q]
  (cond
    (nil? t)
    (disj-from-bucket node q v)

    (= t "*")
    (if-let [child (:pwc node)]
      (let [child' (rm child rst v q)]
        (assoc node :pwc (when-not (dead? child') child')))
      node)

    (= t ">")
    (if-let [fwc (:fwc node)]
      (let [fwc' (disj-from-bucket fwc q v)]
        (assoc node :fwc (when-not (fwc-dead? fwc') fwc')))
      node)

    :else
    (if-let [child (get-in node [:children t])]
      (let [child' (rm child rst v q)]
        (if (dead? child')
          (update node :children dissoc t)
          (assoc-in node [:children t] child')))
      node)))

(defn- contains-sub? [node [t & rst] v q]
  (cond
    (nil? t)
    (if q
      (contains? (get-in node [:qsubs q] #{}) v)
      (contains? (:psubs node) v))

    (= t "*")
    (if-let [c (:pwc node)] (contains-sub? c rst v q) false)

    (= t ">")
    (if-let [fwc (:fwc node)]
      (if q
        (contains? (get-in fwc [:qsubs q] #{}) v)
        (contains? (:psubs fwc) v))
      false)

    :else
    (if-let [c (get-in node [:children t])]
      (contains-sub? c rst v q)
      false)))

;; --- match ---

(def ^:private empty-result {:plain #{} :groups {}})

(defn- merge-results [a b]
  {:plain  (into (:plain a) (:plain b))
   :groups (merge-with into (:groups a) (:groups b))})

(defn- mtch [node [t & rst]]
  (if (nil? t)
    ;; Terminal: no fwc contribution. `>` matches one-or-more tail tokens,
    ;; so it can't fire here. Only exact subs at this node.
    {:plain (:psubs node) :groups (:qsubs node)}
    (let [fwc-contrib (when-let [fwc (:fwc node)]
                        {:plain (:psubs fwc) :groups (:qsubs fwc)})
          pwc-contrib (when-let [pwc (:pwc node)]
                        (mtch pwc rst))
          lit-contrib (when-let [c (get-in node [:children t])]
                        (mtch c rst))]
      (reduce merge-results empty-result
              (keep identity [fwc-contrib pwc-contrib lit-contrib])))))

;; --- public api ---

(defn make
  "Returns a fresh sublist (atom wrapping a trie node)."
  []
  (atom empty-node))

(defn insert!
  "Registers `value` under `subject`. `subject` is a pattern and may contain
   `*` or `>` wildcards. An optional `{:queue name}` places the subscription
   in a queue group."
  ([sl subject value] (insert! sl subject value nil))
  ([sl subject value {:keys [queue]}]
   (let [tokens (validate-pattern subject)]
     (swap! sl ins tokens value queue)
     nil)))

(defn remove!
  "Removes a previously-inserted subscription. Returns `{:removed? bool}`.
   Removing an absent subscription is a no-op."
  ([sl subject value] (remove! sl subject value nil))
  ([sl subject value {:keys [queue]}]
   (let [tokens  (validate-pattern subject)
         [old _] (swap-vals! sl rm tokens value queue)]
     {:removed? (contains-sub? old tokens value queue)})))

(defn match
  "Looks up subscriptions matching `subject`, which must be a literal
   (no `*` or `>`). Returns `{:plain #{...} :groups {queue #{...}}}`."
  [sl subject]
  (let [tokens (validate-subject subject)]
    (mtch @sl tokens)))

(defn pick-one
  "Collapses a match result to a flat delivery set: all `:plain` values
   plus one `rand-nth`-picked value from each queue group."
  [{:keys [plain groups]}]
  (reduce (fn [acc [_ subs]]
            (if (seq subs)
              (conj acc (rand-nth (vec subs)))
              acc))
          plain
          groups))
