(ns toolkit.datapotamus2.dsl-test
  (:require [clojure.test :refer [deftest is testing]]
            [toolkit.datapotamus2.combinators :as c]
            [toolkit.datapotamus2.core :as dp2]
            [toolkit.datapotamus2.dsl :as dsl]))

(defn- events-of [result kind]
  (filterv #(= kind (:kind %)) (:events result)))

;; --- step + serial: smallest composition -----------------------------------

(deftest step-is-a-1-in-1-out-flow
  (let [f (dsl/step :inc (c/wrap inc))]
    (is (= #{:inc} (set (keys (:procs f))))
        "procs map has a single entry keyed by the id")
    (is (= :inc (:in f)))
    (is (= :inc (:out f)))
    (is (= [] (:conns f)))))

(deftest serial-glues-flows-sequentially
  (let [a (dsl/step :inc (c/wrap inc))
        b (dsl/step :dbl (c/wrap #(* 2 %)))
        composed (dsl/serial a b (dsl/step :sink (c/absorb-sink)))
        result   (dp2/run! composed {:data 5})]
    (testing "run completes and sink received the expected data"
      (is (= :completed (:state result)))
      (is (= 12 (:data (first (filterv #(= :sink (:step-id %))
                                       (events-of result :recv)))))))
    (testing "conns were auto-wired"
      (is (= [[[:inc :out] [:dbl :in]]
              [[:dbl :out] [:sink :in]]]
             (:conns composed))))
    (testing "input/output are first/last"
      (is (= :inc  (:in composed)))
      (is (= :sink (:out composed))))))

(deftest serial-rejects-collisions
  (let [a (dsl/step :same (c/wrap inc))
        b (dsl/step :same (c/wrap dec))]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"colliding"
                          (dsl/serial a b)))))

;; --- as-step: black-box composition ---------------------------------------

(deftest deep-nested-as-step
  ;; Three levels of as-step nesting: F → sub1 → sub2 → sub3 → leaf.
  ;; Verifies the scope prefix accumulates correctly and graph ids get
  ;; uniquely prefixed at each level.
  (let [leaf  (dsl/step :leaf (c/wrap inc))
        sub3  (dsl/as-step :sub3 leaf)
        sub2  (dsl/as-step :sub2 sub3)
        sub1  (dsl/as-step :sub1 sub2)
        outer (dsl/serial sub1 (dsl/step :sink (c/absorb-sink)))
        result (dp2/run! outer {:flow-id "F" :data 41})
        leaf-recv (first (filterv #(= :leaf (:step-id %))
                                  (events-of result :recv)))]
    (testing "run completes; sink got (inc 41) = 42"
      (is (= :completed (:state result)))
      (is (= 42 (:data (first (filterv #(= :sink (:step-id %))
                                       (events-of result :recv)))))))
    (testing "leaf step's scope nests one [:flow] per subflow level"
      (is (= [[:flow "F"]
              [:flow "sub1"]
              [:flow "sub2"]
              [:flow "sub3"]
              [:step :leaf]]
             (:scope leaf-recv))))
    (testing ":flow-path is the [:flow] sids in order"
      (is (= ["F" "sub1" "sub2" "sub3"] (:flow-path leaf-recv))))
    (testing ":step-id stays the original :leaf, not the prefixed graph id"
      (is (= :leaf (:step-id leaf-recv))))))

(deftest as-step-namespaces-inner-procs
  (let [inner    (dsl/serial
                  (dsl/step :a (c/wrap inc))
                  (dsl/step :b (c/wrap #(* 10 %))))
        outer    (dsl/serial
                  (dsl/step :pre  (c/wrap identity))
                  (dsl/as-step :sub inner)
                  (dsl/step :post (c/wrap identity))
                  (dsl/step :sink (c/absorb-sink)))
        result   (dp2/run! outer {:flow-id "f" :data 2})
        sink-recv (first (filterv #(= :sink (:step-id %))
                                  (events-of result :recv)))]
    (testing "run completes"
      (is (= :completed (:state result))))
    (testing "data flowed through pre → sub(a,b) → post → sink"
      (is (= 30 (:data sink-recv))))        ; 2 → 2 → (inc 2)=3 → (* 10 3)=30 → 30 → 30
    (testing "inner events have the nested [:flow :sub] scope segment"
      (let [inner-a-recv (first (filterv #(and (= :a (:step-id %))
                                                (= :recv (:kind %)))
                                          (:events result)))]
        (is (= [[:flow "f"] [:flow "sub"] [:step :a]]
               (:scope inner-a-recv)))))
    (testing "outer-level step (e.g. :pre) has only the flow-level scope"
      (let [pre-recv (first (filterv #(and (= :pre (:step-id %))
                                            (= :recv (:kind %)))
                                      (:events result)))]
        (is (= [[:flow "f"] [:step :pre]]
               (:scope pre-recv)))))))

;; --- id-flow: passthrough sanity check ------------------------------------

(deftest id-flow-is-transparent
  (let [flow (dsl/serial
              (dsl/step :inc (c/wrap inc))
              (dsl/id-flow)
              (dsl/step :sink (c/absorb-sink)))
        result (dp2/run! flow {:data 7})]
    (is (= :completed (:state result)))
    (is (= 8 (:data (first (filterv #(= :sink (:step-id %))
                                    (events-of result :recv)))))
        "id-flow forwards (inc 7) = 8 unchanged")))

;; --- merge-flows + connect: explicit wiring -------------------------------

(deftest agent-style-multi-port-with-connect
  ;; Agent-tool pattern built via merge-flows + explicit connects.
  (let [calls (atom 0)
        agent-factory
        (fn [_ctx]
          (fn ([] {:params {}
                   :ins  {:user-in "" :tool-result ""}
                   :outs {:tool-call "" :final ""}})
              ([_] {}) ([s _] s)
              ([s _ m]
               (let [n (swap! calls inc)]
                 (if (< n 3)
                   [s (dsl/emit m :tool-call :query)]
                   [s (dsl/emit m :final     :done)])))))
        flow (-> (dsl/merge-flows
                  (dsl/step :agent agent-factory)
                  (dsl/step :tool  (c/wrap (constantly :tool-response)))
                  (dsl/step :sink  (c/absorb-sink)))
                 (dsl/connect [:agent :tool-call] [:tool :in])
                 (dsl/connect [:tool :out]        [:agent :tool-result])
                 (dsl/connect [:agent :final]     [:sink :in])
                 (dsl/input-at [:agent :user-in])
                 (dsl/output-at :sink))
        result (dp2/run! flow {:data :question})]
    (is (= :completed (:state result)))
    (is (= 3 (count (filterv #(= :agent (:step-id %))
                             (events-of result :recv))))
        "agent recv'd 1 initial + 2 tool results")
    (is (= 1 (count (filterv #(= :sink (:step-id %))
                             (events-of result :recv)))))))

;; --- emit: output-map helper ----------------------------------------------

(deftest emit-single-and-multi-port
  (let [m {:msg-id :fake-parent :data-id :fake-data :data 42
           :tokens {} :parent-msg-ids []}
        single (dsl/emit m :out 7)
        multi  (dsl/emit m :a 1 :b 2)]
    (is (= [:out] (keys single)))
    (is (= 7 (get-in single [:out 0 :data])))
    (is (= #{:a :b} (set (keys multi))))
    (is (= 1 (get-in multi [:a 0 :data])))
    (is (= 2 (get-in multi [:b 0 :data])))))
