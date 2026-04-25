(ns toolkit.datapotamus.latency-test
  (:require [clojure.test :refer [deftest is]]
            [toolkit.pubsub :as pubsub]
            [toolkit.hist :as hist]
            [toolkit.datapotamus.latency :as latency]))

(defn- pub-recv [ps step-id msg-id at]
  (pubsub/pub ps ["recv"] {:kind :recv :step-id step-id :msg-id msg-id :at at}))

(defn- pub-success [ps step-id msg-id at]
  (pubsub/pub ps ["success"] {:kind :success :step-id step-id :msg-id msg-id :at at}))

(defn- pub-failure [ps step-id msg-id at]
  (pubsub/pub ps ["failure"] {:kind :failure :step-id step-id :msg-id msg-id :at at}))

(deftest records-per-step-histograms
  (let [ps (pubsub/make)
        t  (latency/make)]
    (latency/attach! ps t)
    (pub-recv    ps "a" "m1" 100)
    (pub-recv    ps "b" "m2" 200)
    (pub-success ps "a" "m1" 115)
    (pub-failure ps "b" "m2" 300)
    (let [snaps (latency/snapshot t)]
      (is (= #{"a" "b"} (set (keys snaps))))
      (is (= 1 (hist/total-count (get snaps "a"))))
      (is (= 1 (hist/total-count (get snaps "b")))))
    (is (empty? (latency/in-flight t)) "in-flight drains as msgs complete")))

(deftest accumulates-many-durations-per-step
  (let [ps (pubsub/make)
        t  (latency/make)]
    (latency/attach! ps t)
    (dotimes [i 1000]
      (let [mid (str "m" i)]
        (pub-recv    ps "a" mid 0)
        ;; durations 1..1000 ns — sit in the linear (exact) section
        (pub-success ps "a" mid (inc i))))
    (let [snap (get (latency/snapshot t) "a")]
      (is (= 1000 (hist/total-count snap)))
      (is (<= 480 (hist/percentile snap 0.50) 520))
      (is (<= 980 (hist/percentile snap 0.99) 1024)))))

(deftest p99-helper
  (let [ps (pubsub/make)
        t  (latency/make)]
    (latency/attach! ps t)
    (dotimes [i 100]
      (pub-recv    ps "a" (str "m" i) 0)
      (pub-success ps "a" (str "m" i) (inc i)))
    (let [p99 (latency/percentile t 0.99)]
      (is (contains? p99 "a"))
      (is (number? (get p99 "a"))
          "per-step p99 lookup returns a numeric value"))))

(deftest mid-flight-readable
  (let [ps (pubsub/make)
        t  (latency/make)]
    (latency/attach! ps t)
    (pub-recv    ps "a" "m1" 100)
    (pub-recv    ps "a" "m2" 110)
    (pub-success ps "a" "m1" 115)
    (is (= 1 (hist/total-count (get (latency/snapshot t) "a"))))
    (is (= {"m2" 110} (latency/in-flight t)))
    (pub-success ps "a" "m2" 130)
    (is (= 2 (hist/total-count (get (latency/snapshot t) "a"))))
    (is (empty? (latency/in-flight t)))))

(deftest ignores-unrelated-kinds
  (let [ps (pubsub/make)
        t  (latency/make)]
    (latency/attach! ps t)
    (pubsub/pub ps ["send-out"]
                {:kind :send-out :step-id "a" :msg-id "m1" :at 100})
    (pubsub/pub ps ["inject"]
                {:kind :inject :msg-id "m1" :at 100})
    (is (empty? (latency/snapshot t)))
    (is (empty? (latency/in-flight t)))))

(deftest unmatched-success-is-dropped
  (let [ps (pubsub/make)
        t  (latency/make)]
    (latency/attach! ps t)
    ;; success with no prior recv (e.g. attached mid-flight)
    (pub-success ps "a" "m-orphan" 999)
    (is (empty? (latency/snapshot t)) "no histogram created for orphan success")
    (is (empty? (latency/in-flight t)))))

(deftest tap-drives-side-effect
  (let [ps    (pubsub/make)
        t     (latency/make)
        ticks (atom 0)]
    (latency/attach! ps t {:tap (fn [_ _] (swap! ticks inc))})
    (pub-recv    ps "a" "m1" 100)
    (pub-success ps "a" "m1" 115)
    (is (= 2 @ticks) "tap fires once per event")))
