(ns toolkit.hn.tree-fetch
  "Recursive HN tree fetch as a Datapotamus step.

   Takes incoming story-id collections on `:in` and emits one fully
   reassembled tree per story-id on `:out`. Internally uses
   `c/stealing-workers` in recursive-feedback mode (the inner
   handler-map declares a `:work` output port) with K parallel
   workers, each fetching one HN item per invocation and emitting
   child-ids on `:work` for the pool to dispatch back to itself. A
   buffer-until-close aggregator reconstructs each story's tree
   from the flat node stream and emits one merged tree msg per
   story root.

   Per-node observability: each fetch is a real proc invocation
   under the worker's `:step-id`, so visualizers and recorders see
   per-node fan-out, per-node latency, and per-node failures
   without any special status-event hacks.

   Usage:
     (step/serial
       (mk-fetch-top-ids n)              ; emits coll of ids
       (tree-fetch/step {:k 8 :get-json get-json})  ; emits trees
       compute-metrics)

   `:get-json` is the URL→JSON fn (typically `clojure.data.json` over
   `http-kit/get`); supplied as an arg so each pipeline can stub it
   in tests."
  (:require [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]))

(def ^:private base "https://hacker-news.firebaseio.com/v0")

(defn- mk-split-roots
  "Per-input split: each story-id in the incoming coll becomes a
   `{:root id :id id}` envelope — the seed for that story's
   recursive fetch. The :root tag stays with every recursively-emitted
   child so the aggregator can group nodes by their originating story."
  []
  (step/step :split-roots nil
             (fn [ctx _s ids]
               {:out (msg/children ctx (mapv (fn [id] {:root id :id id}) ids))})))

(defn- mk-fetch-node
  "The recursive worker. Receives `{:root story-id :id node-id}`,
   fetches the HN item, emits one node-data envelope on :out and one
   recursive request per kid on :work. c/stealing-workers routes :work
   back into its own queue for any free worker to pick up."
  [get-json]
  (step/handler-map
   {:ports {:ins {:in ""} :outs {:out "" :work ""}}
    :on-data
    (fn [ctx _s {:keys [root id]}]
      (let [item (get-json (str base "/item/" id ".json"))
            kids (or (:kids item) [])]
        {:out  [(msg/child ctx {:root root :node item})]
         :work (mapv (fn [kid-id]
                       (msg/child ctx {:root root :id kid-id}))
                     kids)}))}))

(defn- reassemble-tree
  "Given a flat collection of HN node maps (each with :id and :kids
   from upstream), walk from `root-id` down through :kids and
   return a nested map with `:kid-trees` populated. Nodes missing
   from the map (e.g., a fetch failure) are silently dropped at
   their parent's :kid-trees level."
  [root-id nodes]
  (let [by-id (into {} (map (juxt :id identity)) nodes)]
    ((fn rec [id]
       (when-let [n (by-id id)]
         (assoc n :kid-trees (vec (keep rec (or (:kids n) []))))))
     root-id)))

(def ^:private aggregate
  "Per-tree emit aggregator. For each root, track the set of node ids
   we still expect (initially `#{root}`; grows as we receive nodes
   referencing more kids; shrinks as we receive the kids themselves).
   When the expected set hits empty, the tree is complete — emit one
   `msg/merge` per root and drop that root from state.

   Per-tree emit avoids the close-cascade race that buffer-until-close
   suffers under c/stealing-workers in recursive mode: msg/drain'd :on-data invocations
   absorb the data flow, allowing run-seq's await-quiescent! to fire
   on transient counter balance before :on-all-input-done has run.
   Per-tree emit keeps each tree's emission tied directly to the
   data-flow events that complete it — counter balance moves with
   the merge, not transiently before it.

   :on-all-input-done is a fallback flush for incomplete trees (e.g., a
   fetch failure leaves some kid forever unexpected). It emits any
   partially-reassembled trees so the caller observes the partial
   result rather than silently losing it."
  {:procs
   {:aggregate-trees
    (step/handler-map
     {:ports         {:ins {:in ""} :outs {:out ""}}
      :on-init       (fn [] {})
      :on-data
      (fn [ctx s {:keys [root node]}]
        (let [node-id  (:id node)
              s'       (-> s
                           (update-in [root :entries] (fnil conj [])
                                      {:msg (:msg ctx) :node node})
                           (update-in [root :expected]
                                      (fn [exp]
                                        (-> (or exp #{root})
                                            (disj node-id)
                                            (into (:kids node))))))
              expected (get-in s' [root :expected])]
          (if (empty? expected)
            (let [{:keys [entries]} (get s' root)
                  parents (mapv :msg entries)
                  nodes   (mapv :node entries)
                  tree    (reassemble-tree root nodes)]
              [(dissoc s' root)
               (if tree
                 {:out [(msg/merge ctx parents tree)]}
                 {})])
            [s' msg/drain])))
      :on-all-input-done
      (fn [ctx s]
        ;; Fallback flush: emit any roots that never completed (e.g.
        ;; due to a missing kid from a fetch failure).
        {:out (vec (for [[root {:keys [entries]}] s
                         :let [parents (mapv :msg entries)
                               nodes   (mapv :node entries)
                               tree    (reassemble-tree root nodes)]
                         :when tree]
                     (msg/merge ctx parents tree)))})})}
   :conns [] :in :aggregate-trees :out :aggregate-trees})

(defn step
  "Returns a Datapotamus step that takes a collection of HN
   story-ids on its :in and emits one fully-fetched tree per
   story-id on its :out.

   Options:
     :k          — number of parallel worker procs.
     :get-json   — fn (str → map) to fetch+parse a URL. Tests stub.
     :id         — sid for the wrapping serial step (default :tree-fetch).
                   Each sid in the trace ([:scope id :step ...]) appears
                   under [:scope id]."
  [{:keys [k get-json id]
    :or   {id :tree-fetch}}]
  (assert (pos-int? k) "tree-fetch: :k must be a positive integer")
  (assert (ifn? get-json) "tree-fetch: :get-json must be a function")
  (step/serial id
               (mk-split-roots)
               (c/stealing-workers :tree-fetchers k (mk-fetch-node get-json))
               aggregate))
