;; Refs:
;; - https://andersmurphy.com/2025/04/15/why-you-should-use-brotli-sse.html
;; - https://github.com/JeremS/datastar-pedestal-http-kit

(ns datastar.core
  (:require
   [clojure.data.json :as json]
   [clojure.string :as string]
   [datastar.sse]
   [hiccup2.core :as h]
   [hiccup.util :as hu]))

;; re-export sse functions for user convenience.
;; exported as vars for interactive development.
;; claude says the runtime cost is a few pointer dereferences per call.
(def sse-stream   #'datastar.sse/stream)
(def sse-response #'datastar.sse/response)
(def sse-send!    #'datastar.sse/send!)
(def sse-close!   #'datastar.sse/close!)

(defn- elements-list
  "Given a hiccup html representation, return a string of datastar-style 'elements' data lines"
  [elems]
  (let [s (str (h/html elems))]
    (when (not-empty s)
      (map (partial str "elements ") (string/split-lines s)))))

(defn data-lines
  "Takes a vector of data and returns a vector of SSE-ready data lines prefixed with 'data: '"
  [data]
  (map #(str "data: " % "\n") data))

;; turns dispatch opts into the lines we want to send via sse
(defn lines [{:keys [:name :id :retry :data]}]
  (cond-> []
    name                          (conj (str "event: " name "\n"))
    id                            (conj (str "id: " id "\n"))
    (and retry (not= 1000 retry)) (conj (str "retry: " retry "\n"))
    data                          (into (data-lines data))
    true                          (conj "\n\n")))

(defn send-event!
  "Send an SSE event in datastar format to the channel ch.
   Accepts event :name, :id, :retry, :data.

   See https://github.com/starfederation/datastar/blob/main/sdk/ADR.md#serversenteventgeneratorsend"
  [sse opts]
  (datastar.sse/send! sse (lines opts)))

(defn- patch-elements-core
  [elements & {:keys [selector mode use-view-transition namespace] :as opts}]
  (let [data (cond-> []
               mode                (conj (str "mode " mode))
               selector            (conj (str "selector " selector))
               namespace           (conj (str "namespace " namespace))
               use-view-transition (conj (str "useViewTransition " use-view-transition))
               elements            (into (elements-list elements)))]
    (assoc opts
           :name "datastar-patch-elements"
           :data data)))

(defn patch-elements
  "See https://github.com/starfederation/datastar/blob/main/sdk/ADR.md#serversenteventgeneratorpatchelements"
  ([sse elements]      (send-event! sse (patch-elements-core elements)))
  ([sse elements opts] (send-event! sse (patch-elements-core elements opts))))

(defn- patch-signals-core [signals & {:keys [only-if-missing] :as opts}]
  (let [data (cond-> []
               only-if-missing (conj "onlyIfMissing true")
               true            (conj (str "signals " (json/write-str signals))))]
    (assoc opts
           :name "datastar-patch-signals"
           :data data)))

(defn patch-signals
  "See https://github.com/starfederation/datastar/blob/main/sdk/ADR.md#serversenteventgeneratorpatchsignals"
  ([sse signals] (send-event! sse (patch-signals-core signals)))
  ([sse signals opts] (send-event! sse (patch-signals-core signals opts))))

(defn- execute-script-core
  "Emit a script element. If both attrs and auto-remove are specified, a :data-effect key in
   attrs will override the auto-remove data-effect attribute added by this function.

   See https://github.com/starfederation/datastar/blob/main/sdk/ADR.md#serversenteventgeneratorexecutescript"
  [script & {:keys [attrs auto-remove] :as opts}]
  (patch-elements-core
   [:script
    (merge (when auto-remove {:data-effect "el.remove()"}) attrs)
    (hu/raw-string script)]
   (merge opts {:mode "append" :selector "body"})))

(defn execute-script
  "Emit a script element. If both attrs and auto-remove are specified, a :data-effect key in
   attrs will override the auto-remove data-effect attribute added by this function.

   See https://github.com/starfederation/datastar/blob/main/sdk/ADR.md#serversenteventgeneratorexecutescript"
  [sse script & [opts]] (send-event! sse (execute-script-core script opts)))

(defn read-signals
  "See https://github.com/starfederation/datastar/blob/main/sdk/ADR.md#readsignals"
  [request]
  (let [ret (if (and (contains? #{:get :delete} (:request-method request)) (contains? (:query-params request) :datastar))
              ;; http method get: assume for now a json datastar parameter
              (json/read-str (:datastar (:query-params request)))
              ;; other methods: assume a json body
              (:json-params request))]
    ret))

;; perf opportunities:
;; - maintain the data as a vector through to the event->bytes method
;; which would iterate through the vector of strings and split into
;; lines at that point, rather than joining a vector of strings here
;; only to split them again soon after. to do this we need to change
;; use of sse/extract-string in sse in order to extracting a string
;; or vec in the specific case of the :data key.
;; - elements-list: optimize the common case of one-line strings

;; testing
#_(patch-elements-core [:h1 "fooo"] {:selector "#roop"})
#_(patch-signals-core {:a 1 :b 2} {:only-if-missing true})
#_(println (execute-script-core "alert(1)" nil))
