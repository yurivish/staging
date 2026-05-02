(ns toolkit.datapotamus.polling-test
  "Tests for `flow/run-polling!` — a driver that wraps a step in an
   output collector, starts the flow, and pumps polled values via
   `inject!` from a virtual thread. Returns immediately; caller is
   responsible for `stop!`. Suitable for unbounded streaming sources
   that don't naturally drain (live HN firehose etc.)."
  (:refer-clojure :exclude [run!])
  (:require [clojure.test :refer [deftest is testing]]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.step :as step]))

;; --- A. Bounded run via :max-ticks ----------------------------------------

(deftest a1-emits-polled-values-through-pipeline
  ;; poll-fn yields one item per tick: counter 1, 2, 3, ...
  (let [pipe (step/step :double (fn [n] (* 2 n)))
        h    (flow/run-polling! pipe
                                {:interval-ms 5
                                 :init-state  {:n 0}
                                 :poll-fn     (fn [{:keys [n]}]
                                                [{:n (inc n)} [(inc n)]])
                                 :max-ticks   5})]
    (try
      ;; Wait for 5 ticks (5 * 5ms + slack).
      (Thread/sleep 200)
      (let [observed (sort (mapv :data @(:collected h)))]
        (is (= [2 4 6 8 10] observed) "doubled values 1..5"))
      (finally
        ((:stop! h))))))

;; --- B. stop! is idempotent and halts polling -----------------------------

(deftest b1-stop-halts-polling
  (let [counter (atom 0)
        h (flow/run-polling! (step/step :id identity)
                             {:interval-ms 5
                              :init-state  {}
                              :poll-fn     (fn [s]
                                             (swap! counter inc)
                                             [s [@counter]])})]
    (Thread/sleep 50)  ;; collect a few ticks
    (let [pre-stop @counter]
      ((:stop! h))
      (Thread/sleep 50)  ;; ensure no more ticks fire
      (let [post-stop @counter]
        (is (>= pre-stop 2) "polled at least twice before stop")
        (is (<= (- post-stop pre-stop) 1)
            (str "no significant ticks after stop; pre=" pre-stop " post=" post-stop)))
      (testing "second stop is safe"
        ((:stop! h))))))

;; --- C. State threads through poll-fn -------------------------------------

(deftest c1-poll-fn-state-monotone
  ;; State accumulates a count; emit the running count each tick.
  (let [h (flow/run-polling! (step/step :id identity)
                             {:interval-ms 5
                              :init-state  {:n 0}
                              :poll-fn     (fn [{:keys [n]}]
                                             (let [n' (inc n)]
                                               [{:n n'} [n']]))
                              :max-ticks   4})]
    (try
      (Thread/sleep 150)
      (let [seen (sort (mapv :data @(:collected h)))]
        (is (= [1 2 3 4] seen) "monotone state-derived values"))
      (finally
        ((:stop! h))))))

;; --- D. Multiple values per tick ------------------------------------------

(deftest d1-poll-fn-can-emit-multiple-values-per-tick
  (let [h (flow/run-polling! (step/step :id identity)
                             {:interval-ms 5
                              :poll-fn     (fn [s] [s [:a :b :c]])
                              :max-ticks   3})]
    (try
      (Thread/sleep 150)
      (let [seen @(:collected h)]
        (is (= 9 (count seen)) "3 ticks * 3 values = 9 outputs"))
      (finally
        ((:stop! h))))))
